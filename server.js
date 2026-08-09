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

const connectedUsers = {};

io.on('connection', (socket) => {

    socket.on('login', (data) => {
        const { username, password, pfp, isSignup } = data;

        db.get('SELECT * FROM users WHERE username = ?', [username], (err, row) => {
            if (err) return socket.emit('loginResponse', { success: false, msg: 'Database error!' });

            if (isSignup) {
                if (row) {
                    return socket.emit('loginResponse', { success: false, msg: 'Username already taken. Please choose another or log in.' });
                }
                db.run('INSERT INTO users (username, password, pfp) VALUES (?, ?, ?)', [username, password, pfp], (err) => {
                    if (err) socket.emit('loginResponse', { success: false, msg: 'Account creation failed.' });
                    else completeLogin(pfp);
                });
            } else {
                if (!row) {
                    return socket.emit('loginResponse', { success: false, msg: 'Account not found. Please sign up first!' });
                }
                if (row.password === password) {
                    if (pfp && pfp !== '👤') {
                        db.run('UPDATE users SET pfp = ? WHERE username = ?', [pfp, username]);
                    }
                    completeLogin(row.pfp || pfp);
                } else {
                    socket.emit('loginResponse', { success: false, msg: 'Incorrect password!' });
                }
            }
        });

        function completeLogin(userPfp) {
            socket.username = username;
            socket.pfp = userPfp;
            connectedUsers[username] = socket.id;

            socket.emit('loginResponse', { success: true, user: { username, pfp: userPfp } });
            io.emit('updateUserList', getUsersList());
        }
    });

    function getUsersList() {
        return Object.keys(connectedUsers).map(u => ({
            username: u,
            pfp: io.sockets.sockets.get(connectedUsers[u])?.pfp || '👤'
        }));
    }

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
                if (connectedUsers[target]) {
                    io.to(connectedUsers[target]).emit('messagesMarkedRead', { by: socket.username });
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
        if (data.recipient && connectedUsers[data.recipient]) {
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
                if (connectedUsers[data.recipient]) {
                    io.to(connectedUsers[data.recipient]).emit('receiveMessage', payload);
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
                        if (connectedUsers[target]) io.to(connectedUsers[target]).emit('reactionUpdated', updatePayload);
                        socket.emit('reactionUpdated', updatePayload);
                    }
                });
            }
        });
    });

    socket.on('typing', ({ target, isGroup, isTyping }) => {
        if (isGroup) {
            socket.to(target).emit('userTyping', { from: socket.username, room: target, isTyping });
        } else if (connectedUsers[target]) {
            io.to(connectedUsers[target]).emit('userTyping', { from: socket.username, isTyping });
        }
    });

    socket.on('markAsRead', ({ sender }) => {
        db.run(`UPDATE messages SET status = 'read' WHERE sender = ? AND recipient = ? AND status != 'read'`, [sender, socket.username], () => {
            if (connectedUsers[sender]) {
                io.to(connectedUsers[sender]).emit('messagesMarkedRead', { by: socket.username });
            }
        });
    });

    socket.on('startCallNotification', ({ target, isGroup, callRoomName, isAudioOnly, senderPfp }) => {
        if (isGroup) {
            socket.to(target).emit('incomingCall', {
                from: socket.username,
                callRoomName,
                isAudioOnly,
                isGroup: true,
                groupName: target,
                senderPfp
            });
        } else {
            if (connectedUsers[target]) {
                io.to(connectedUsers[target]).emit('incomingCall', {
                    from: socket.username,
                    callRoomName,
                    isAudioOnly,
                    isGroup: false,
                    senderPfp
                });
            }
        }
    });

    socket.on('declineCall', ({ to }) => {
        if (connectedUsers[to]) {
            io.to(connectedUsers[to]).emit('callDeclined', { from: socket.username });
        }
    });

    socket.on('disconnect', () => {
        if (socket.username) {
            delete connectedUsers[socket.username];
            io.emit('updateUserList', getUsersList());
        }
    });
});

const PORT = process.env.PORT || 3000;
http.listen(PORT, '0.0.0.0', () => console.log(`WhatsApp Chatz running on port ${PORT}`));