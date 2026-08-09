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

app.get('/', (req, res) => res.sendFile(__dirname + '/index.html'));

// Maps normalized username -> socket id
const connectedUsers = {};

function broadcastUserList() {
    db.all('SELECT username, pfp FROM users', [], (err, rows) => {
        if (err) return;
        const allUsers = (rows || []).map(row => ({
            username: row.username,
            pfp: row.pfp || '👤',
            isOnline: !!connectedUsers[row.username.toLowerCase()]
        }));
        io.emit('updateUserList', allUsers);
    });
}

io.on('connection', (socket) => {

    socket.on('login', (data) => {
        const rawUsername = (data.username || '').trim();
        const password = data.password || '';
        const pfp = data.pfp || '👤';

        if (!rawUsername) {
            return socket.emit('loginResponse', { success: false, msg: 'Username cannot be empty.' });
        }

        const normUser = rawUsername.toLowerCase();

        db.get('SELECT * FROM users WHERE LOWER(username) = ?', [normUser], (err, row) => {
            if (err) return socket.emit('loginResponse', { success: false, msg: 'Database error!' });

            if (row) {
                if (row.password === password) {
                    db.run('UPDATE users SET pfp = ? WHERE LOWER(username) = ?', [pfp, normUser]);
                    completeLogin(row.username, pfp);
                } else {
                    socket.emit('loginResponse', { success: false, msg: 'Incorrect password!' });
                }
            } else {
                db.run('INSERT INTO users (username, password, pfp) VALUES (?, ?, ?)', [rawUsername, password, pfp], (err) => {
                    if (err) socket.emit('loginResponse', { success: false, msg: 'Account creation failed.' });
                    else completeLogin(rawUsername, pfp);
                });
            }
        });

        function completeLogin(username, userPfp) {
            socket.username = username;
            socket.pfp = userPfp;
            connectedUsers[username.toLowerCase()] = socket.id;

            socket.emit('loginResponse', { success: true, user: { username, pfp: userPfp } });
            broadcastUserList();
        }
    });

    socket.on('joinGroup', (roomName) => {
        if (!socket.username || !roomName) return;
        const room = roomName.trim();
        socket.join(room);
        socket.emit('groupJoined', room);
    });

    socket.on('loadConversation', ({ target, isGroup }) => {
        if (!socket.username) return;

        let query, params;
        if (isGroup) {
            query = `SELECT id, sender, recipient, room, message, fileData, fileName, fileType, status, reactions, strftime('%H:%M', timestamp, 'localtime') as time FROM messages WHERE room = ? ORDER BY id ASC`;
            params = [target];
        } else {
            query = `SELECT id, sender, recipient, room, message, fileData, fileName, fileType, status, reactions, strftime('%H:%M', timestamp, 'localtime') as time FROM messages WHERE (LOWER(sender) = ? AND LOWER(recipient) = ?) OR (LOWER(sender) = ? AND LOWER(recipient) = ?) ORDER BY id ASC`;
            const me = socket.username.toLowerCase();
            const other = target.toLowerCase();
            params = [me, other, other, me];

            db.run(`UPDATE messages SET status = 'read' WHERE LOWER(sender) = ? AND LOWER(recipient) = ? AND status != 'read'`, [other, me], () => {
                const targetSocketId = connectedUsers[other];
                if (targetSocketId) {
                    io.to(targetSocketId).emit('messagesMarkedRead', { by: socket.username });
                }
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
        
        let initialStatus = 'sent';
        const recipientNorm = data.recipient ? data.recipient.toLowerCase() : null;
        if (recipientNorm && connectedUsers[recipientNorm]) {
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
                if (connectedUsers[recipientNorm]) {
                    io.to(connectedUsers[recipientNorm]).emit('receiveMessage', payload);
                }
                socket.emit('receiveMessage', payload);
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
                        const targetNorm = target.toLowerCase();
                        if (connectedUsers[targetNorm]) io.to(connectedUsers[targetNorm]).emit('reactionUpdated', updatePayload);
                        socket.emit('reactionUpdated', updatePayload);
                    }
                });
            }
        });
    });

    socket.on('typing', ({ target, isGroup, isTyping }) => {
        if (isGroup) {
            socket.to(target).emit('userTyping', { from: socket.username, room: target, isTyping });
        } else {
            const targetNorm = target.toLowerCase();
            if (connectedUsers[targetNorm]) {
                io.to(connectedUsers[targetNorm]).emit('userTyping', { from: socket.username, isTyping });
            }
        }
    });

    socket.on('markAsRead', ({ sender }) => {
        const me = socket.username.toLowerCase();
        const senderNorm = sender.toLowerCase();
        db.run(`UPDATE messages SET status = 'read' WHERE LOWER(sender) = ? AND LOWER(recipient) = ? AND status != 'read'`, [senderNorm, me], () => {
            if (connectedUsers[senderNorm]) {
                io.to(connectedUsers[senderNorm]).emit('messagesMarkedRead', { by: socket.username });
            }
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
            const targetNorm = target.toLowerCase();
            if (connectedUsers[targetNorm]) {
                io.to(connectedUsers[targetNorm]).emit('incomingCall', {
                    from: socket.username,
                    callRoomName,
                    isAudioOnly,
                    isGroup: false
                });
            }
        }
    });

    socket.on('declineCall', ({ to }) => {
        const toNorm = to.toLowerCase();
        if (connectedUsers[toNorm]) {
            io.to(connectedUsers[toNorm]).emit('callDeclined', { from: socket.username });
        }
    });

    socket.on('disconnect', () => {
        if (socket.username) {
            delete connectedUsers[socket.username.toLowerCase()];
            broadcastUserList();
        }
    });
});

const PORT = process.env.PORT || 3000;
http.listen(PORT, () => console.log(`WhatsApp Chatz running on http://localhost:${PORT}`));