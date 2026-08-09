const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
    maxHttpBufferSize: 1e8,
    cors: { origin: "*", methods: ["GET", "POST"] },
    pingInterval: 10000,
    pingTimeout: 5000
});
const sqlite3 = require('sqlite3').verbose();

const db = new sqlite3.Database('./chat.db');
db.run('PRAGMA journal_mode = WAL;');

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, pfp TEXT)`);
    db.run(`CREATE TABLE IF NOT EXISTS blocks (blocker TEXT, blocked TEXT, PRIMARY KEY (blocker, blocked))`);
    db.run(`CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        sender TEXT, 
        recipient TEXT, 
        room TEXT, 
        message TEXT, 
        fileData TEXT, 
        fileName TEXT, 
        fileType TEXT, 
        status TEXT DEFAULT 'sent', 
        reactions TEXT DEFAULT '{}',
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);
});

app.use(express.static(__dirname));
app.get('/', (req, res) => res.sendFile(__dirname + '/index.html'));

const onlineUsers = new Set();

function broadcastUserList() {
    db.all('SELECT username, pfp FROM users', [], (err, rows) => {
        if (err) return;
        const userList = rows.map(u => ({
            username: u.username,
            pfp: u.pfp || '👤',
            online: onlineUsers.has(u.username)
        }));
        io.emit('updateUserList', userList);
    });
}

function isBlocked(blocker, blocked, callback) {
    db.get('SELECT 1 FROM blocks WHERE blocker = ? AND blocked = ?', [blocker, blocked], (err, row) => {
        callback(!!row);
    });
}

io.on('connection', (socket) => {

    socket.on('login', (data) => {
        const { username, password, pfp } = data;
        if (!username || !password) {
            return socket.emit('loginResponse', { success: false, msg: 'Username and password required!' });
        }

        db.get('SELECT * FROM users WHERE username = ?', [username], (err, row) => {
            if (err) return socket.emit('loginResponse', { success: false, msg: 'Database error!' });

            if (row) {
                if (row.password === password) {
                    const updatedPfp = (pfp && pfp !== '👤') ? pfp : (row.pfp || '👤');
                    db.run('UPDATE users SET pfp = ? WHERE username = ?', [updatedPfp, username]);
                    completeLogin(updatedPfp);
                } else {
                    socket.emit('loginResponse', { success: false, msg: 'Incorrect password!' });
                }
            } else {
                const initialPfp = pfp || '👤';
                db.run('INSERT INTO users (username, password, pfp) VALUES (?, ?, ?)', [username, password, initialPfp], (err) => {
                    if (err) socket.emit('loginResponse', { success: false, msg: 'Account creation failed.' });
                    else completeLogin(initialPfp);
                });
            }
        });

        function completeLogin(finalPfp) {
            socket.username = username;
            socket.pfp = finalPfp;
            
            // Join multi-device personal account room
            socket.join(`user:${username}`);
            onlineUsers.add(username);

            socket.emit('loginResponse', { success: true, user: { username, pfp: finalPfp } });
            broadcastUserList();
            sendBlockList(username);
        }
    });

    function sendBlockList(username) {
        db.all('SELECT blocked FROM blocks WHERE blocker = ?', [username], (err, rows) => {
            if (!err) {
                const list = rows.map(r => r.blocked);
                io.to(`user:${username}`).emit('blockListUpdated', list);
            }
        });
    }

    socket.on('blockUser', (target) => {
        if (!socket.username || !target) return;
        db.run('INSERT OR IGNORE INTO blocks (blocker, blocked) VALUES (?, ?)', [socket.username, target], () => {
            sendBlockList(socket.username);
        });
    });

    socket.on('unblockUser', (target) => {
        if (!socket.username || !target) return;
        db.run('DELETE FROM blocks WHERE blocker = ? AND blocked = ?', [socket.username, target], () => {
            sendBlockList(socket.username);
        });
    });

    socket.on('joinGroup', (roomName) => {
        if (!socket.username || !roomName) return;
        socket.join(roomName);
        socket.emit('groupJoined', roomName);
    });

    socket.on('loadConversation', ({ target, isGroup }) => {
        if (!socket.username) return;

        let query, params;
        if (isGroup) {
            query = `SELECT id, sender, recipient, room, message, fileData, fileName, fileType, status, reactions, strftime('%H:%M', timestamp, 'localtime') as time FROM messages WHERE room = ? ORDER BY id ASC`;
            params = [target];
        } else {
            query = `SELECT id, sender, recipient, room, message, fileData, fileName, fileType, status, reactions, strftime('%H:%M', timestamp, 'localtime') as time FROM messages WHERE (sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?) ORDER BY id ASC`;
            params = [socket.username, target, target, socket.username];

            db.run(`UPDATE messages SET status = 'read' WHERE sender = ? AND recipient = ? AND status != 'read'`, [target, socket.username], () => {
                io.to(`user:${target}`).emit('messagesMarkedRead', { by: socket.username });
            });
        }

        db.all(query, params, (err, rows) => {
            if (!err) {
                socket.emit('conversationHistory', { target, isGroup, history: rows || [] });
            }
        });
    });

    socket.on('sendMessage', (data) => {
        if (!socket.username) return;

        const now = new Date();
        const timeStr = now.getHours().toString().padStart(2, '0') + ':' + now.getMinutes().toString().padStart(2, '0');

        if (data.recipient) {
            isBlocked(data.recipient, socket.username, (blocked) => {
                if (blocked) return;
                processMessageInsert();
            });
        } else {
            processMessageInsert();
        }

        function processMessageInsert() {
            let initialStatus = 'sent';
            if (data.recipient && onlineUsers.has(data.recipient)) {
                initialStatus = 'delivered';
            }

            const stmt = db.prepare('INSERT INTO messages (sender, recipient, room, message, fileData, fileName, fileType, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
            stmt.run(socket.username, data.recipient || null, data.room || null, data.message || '', data.fileData || '', data.fileName || '', data.fileType || '', initialStatus, function(err) {
                if (err) return;

                const insertedId = this.lastID;
                const payload = {
                    id: insertedId,
                    sender: socket.username,
                    recipient: data.recipient || null,
                    room: data.room || null,
                    message: data.message || '',
                    fileData: data.fileData || '',
                    fileName: data.fileName || '',
                    fileType: data.fileType || '',
                    status: initialStatus,
                    reactions: '{}',
                    time: timeStr
                };

                if (data.room) {
                    io.to(data.room).emit('receiveMessage', payload);
                } else if (data.recipient) {
                    // Sync message immediately to ALL devices of sender and recipient
                    io.to(`user:${data.recipient}`).emit('receiveMessage', payload);
                    io.to(`user:${socket.username}`).emit('receiveMessage', payload);
                }
            });
        }
    });

    socket.on('deleteMessage', ({ msgId, target, isGroup }) => {
        if (!socket.username) return;
        db.run('DELETE FROM messages WHERE id = ? AND sender = ?', [msgId, socket.username], function(err) {
            if (!err && this.changes > 0) {
                const payload = { msgId, target, isGroup };
                if (isGroup) {
                    io.to(target).emit('messageDeleted', payload);
                } else {
                    io.to(`user:${target}`).emit('messageDeleted', payload);
                    io.to(`user:${socket.username}`).emit('messageDeleted', payload);
                }
            }
        });
    });

    socket.on('addReaction', ({ msgId, emoji, target, isGroup }) => {
        db.get(`SELECT reactions FROM messages WHERE id = ?`, [msgId], (err, row) => {
            if (row) {
                let reactions = JSON.parse(row.reactions || '{}');
                reactions[socket.username] = emoji;
                const updatedStr = JSON.stringify(reactions);

                db.run(`UPDATE messages SET reactions = ? WHERE id = ?`, [updatedStr, msgId], () => {
                    const updatePayload = { msgId, reactions: updatedStr };
                    if (isGroup) {
                        io.to(target).emit('reactionUpdated', updatePayload);
                    } else {
                        io.to(`user:${target}`).emit('reactionUpdated', updatePayload);
                        io.to(`user:${socket.username}`).emit('reactionUpdated', updatePayload);
                    }
                });
            }
        });
    });

    socket.on('typing', ({ target, isGroup, isTyping }) => {
        if (isGroup) {
            socket.to(target).emit('userTyping', { from: socket.username, room: target, isTyping });
        } else {
            io.to(`user:${target}`).emit('userTyping', { from: socket.username, isTyping });
        }
    });

    socket.on('markAsRead', ({ sender }) => {
        db.run(`UPDATE messages SET status = 'read' WHERE sender = ? AND recipient = ? AND status != 'read'`, [sender, socket.username], () => {
            io.to(`user:${sender}`).emit('messagesMarkedRead', { by: socket.username });
        });
    });

    socket.on('startCallNotification', ({ target, isGroup, callRoomName, isAudioOnly }) => {
        if (isGroup) {
            socket.to(target).emit('incomingCall', {
                from: socket.username,
                callRoomName,
                isAudioOnly,
                isGroup: true,
                groupName: target
            });
        } else {
            isBlocked(target, socket.username, (blocked) => {
                if (!blocked) {
                    io.to(`user:${target}`).emit('incomingCall', {
                        from: socket.username,
                        callRoomName,
                        isAudioOnly,
                        isGroup: false
                    });
                }
            });
        }
    });

    socket.on('declineCall', ({ to }) => {
        io.to(`user:${to}`).emit('callDeclined', { from: socket.username });
    });

    socket.on('disconnect', () => {
        if (socket.username) {
            const userRoom = io.sockets.adapter.rooms.get(`user:${socket.username}`);
            if (!userRoom || userRoom.size === 0) {
                onlineUsers.delete(socket.username);
                broadcastUserList();
            }
        }
    });
});

const PORT = process.env.PORT || 3000;
http.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));