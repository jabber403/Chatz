const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
    maxHttpBufferSize: 1e8, // 100MB max payload for files/voice notes
    cors: { 
        origin: "*", 
        methods: ["GET", "POST"] 
    },
    pingInterval: 10000,
    pingTimeout: 5000
});
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

// Initialize SQLite Database with Write-Ahead Logging (WAL) for concurrency
const db = new sqlite3.Database('./chat.db', (err) => {
    if (err) {
        console.error('Failed to connect to SQLite database:', err.message);
    } else {
        console.log('Connected to SQLite database successfully.');
    }
});

db.run('PRAGMA journal_mode = WAL;');

// Database Schema Initialization
db.serialize(() => {
    // Users table with profile picture support
    db.run(`CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, 
        password TEXT NOT NULL, 
        pfp TEXT DEFAULT '👤',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    // Messages table supporting E2E, media, reactions, status tracking
    db.run(`CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        sender TEXT NOT NULL, 
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

    // Group memberships tracking
    db.run(`CREATE TABLE IF NOT EXISTS group_members (
        room TEXT NOT NULL,
        username TEXT NOT NULL,
        joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (room, username)
    )`);
});

// Middleware & Static File Serving
app.use(express.json());
app.use(express.static(__dirname));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// Real-Time Socket Connection & State Management
const onlineUsers = new Map(); // Username -> Set of socket IDs (multi-device support)

/**
 * Broadcasts full user directory with online/offline indicators to all connected clients
 */
function broadcastUserDirectory() {
    db.all('SELECT username, pfp FROM users', [], (err, rows) => {
        if (err || !rows) return;
        const userList = rows.map(u => ({
            username: u.username,
            pfp: u.pfp || '👤',
            online: onlineUsers.has(u.username) && onlineUsers.get(u.username).size > 0
        }));
        io.emit('updateUserList', userList);
    });
}

/**
 * Helper to fetch unread counts per contact for a given recipient
 */
function fetchUnreadCounts(username, callback) {
    const query = `
        SELECT sender, COUNT(*) as unread_count 
        FROM messages 
        WHERE recipient = ? AND status != 'read' 
        GROUP BY sender
    `;
    db.all(query, [username], (err, rows) => {
        if (err) return callback({});
        const counts = {};
        rows.forEach(row => {
            counts[row.sender] = row.unread_count;
        });
        callback(counts);
    });
}

io.on('connection', (socket) => {
    console.log(`[Socket Connected]: ${socket.id}`);

    // --- USER AUTHENTICATION & LOGIN ---
    socket.on('login', (data) => {
        const { username, password, pfp } = data;

        if (!username || !password) {
            return socket.emit('loginResponse', { 
                success: false, 
                msg: 'Username and password are required.' 
            });
        }

        const trimmedUser = username.trim();

        db.get('SELECT * FROM users WHERE username = ?', [trimmedUser], (err, row) => {
            if (err) {
                return socket.emit('loginResponse', { success: false, msg: 'Database query error.' });
            }

            if (row) {
                // Existing User Authentication
                if (row.password === password) {
                    const finalPfp = (pfp && pfp !== '👤') ? pfp : (row.pfp || '👤');
                    db.run('UPDATE users SET pfp = ? WHERE username = ?', [finalPfp, trimmedUser], () => {
                        completeUserSession(trimmedUser, finalPfp);
                    });
                } else {
                    socket.emit('loginResponse', { success: false, msg: 'Invalid password!' });
                }
            } else {
                // New User Registration
                const initialPfp = pfp || '👤';
                db.run('INSERT INTO users (username, password, pfp) VALUES (?, ?, ?)', [trimmedUser, password, initialPfp], (err) => {
                    if (err) {
                        return socket.emit('loginResponse', { success: false, msg: 'Could not create account.' });
                    }
                    completeUserSession(trimmedUser, initialPfp);
                });
            }
        });

        function completeUserSession(user, userPfp) {
            socket.username = user;
            socket.pfp = userPfp;

            // Register socket in online users map
            if (!onlineUsers.has(user)) {
                onlineUsers.set(user, new Set());
            }
            onlineUsers.get(user).add(socket.id);
            socket.join(`user:${user}`);

            // Automatically rejoin all saved groups
            db.all('SELECT room FROM group_members WHERE username = ?', [user], (err, rows) => {
                const groups = rows ? rows.map(r => r.room) : [];
                groups.forEach(g => socket.join(g));

                // Mark any offline messages pending for this user as 'delivered'
                db.run(`UPDATE messages SET status = 'delivered' WHERE recipient = ? AND status = 'sent'`, [user], () => {
                    fetchUnreadCounts(user, (unreadMap) => {
                        socket.emit('loginResponse', {
                            success: true,
                            user: { username: user, pfp: userPfp },
                            groups,
                            unreadMap
                        });
                        broadcastUserDirectory();
                    });
                });
            });
        }
    });

    // --- GROUP CHAT MANAGEMENT ---
    socket.on('joinGroup', (roomName) => {
        if (!socket.username || !roomName) return;
        const cleanRoom = roomName.trim().toLowerCase();

        db.run('INSERT OR IGNORE INTO group_members (room, username) VALUES (?, ?)', [cleanRoom, socket.username], () => {
            socket.join(cleanRoom);
            socket.emit('groupJoined', cleanRoom);
        });
    });

    // --- MESSAGE HISTORY LOADER ---
    socket.on('loadConversation', ({ target, isGroup }) => {
        if (!socket.username || !target) return;

        if (isGroup) {
            const query = `
                SELECT id, sender, recipient, room, message, fileData, fileName, fileType, status, reactions, strftime('%H:%M', timestamp, 'localtime') as time 
                FROM messages 
                WHERE room = ? 
                ORDER BY id ASC
            `;
            db.all(query, [target], (err, rows) => {
                if (!err) {
                    socket.emit('conversationHistory', { target, isGroup: true, history: rows || [] });
                }
            });
        } else {
            const query = `
                SELECT id, sender, recipient, room, message, fileData, fileName, fileType, status, reactions, strftime('%H:%M', timestamp, 'localtime') as time 
                FROM messages 
                WHERE (sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?) 
                ORDER BY id ASC
            `;
            db.all(query, [socket.username, target, target, socket.username], (err, rows) => {
                if (err) return;

                // Update unread status to 'read' when opening conversation
                db.run(`UPDATE messages SET status = 'read' WHERE sender = ? AND recipient = ? AND status != 'read'`, [target, socket.username], () => {
                    // Inform the sender their messages were read
                    io.to(`user:${target}`).emit('messagesMarkedRead', { by: socket.username });
                    socket.emit('conversationHistory', { target, isGroup: false, history: rows || [] });
                });
            });
        }
    });

    // --- REAL-TIME & OFFLINE MESSAGE TRANSMISSION ---
    socket.on('sendMessage', (data) => {
        if (!socket.username) return;

        const now = new Date();
        const timeStr = now.getHours().toString().padStart(2, '0') + ':' + now.getMinutes().toString().padStart(2, '0');

        const recipient = data.recipient || null;
        const room = data.room || null;
        const isRecipientOnline = recipient && onlineUsers.has(recipient) && onlineUsers.get(recipient).size > 0;

        let initialStatus = 'sent';
        if (isRecipientOnline) {
            initialStatus = 'delivered';
        }

        const query = `
            INSERT INTO messages (sender, recipient, room, message, fileData, fileName, fileType, status) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `;

        db.run(query, [
            socket.username, 
            recipient, 
            room, 
            data.message || '', 
            data.fileData || '', 
            data.fileName || '', 
            data.fileType || '', 
            initialStatus
        ], function(err) {
            if (err) {
                console.error('Error saving message:', err.message);
                return;
            }

            const insertedId = this.lastID;
            const messagePayload = {
                id: insertedId,
                sender: socket.username,
                recipient,
                room,
                message: data.message || '',
                fileData: data.fileData || '',
                fileName: data.fileName || '',
                fileType: data.fileType || '',
                status: initialStatus,
                reactions: '{}',
                time: timeStr
            };

            if (room) {
                // Broadcast to Group Room
                io.to(room).emit('receiveMessage', messagePayload);
            } else if (recipient) {
                // Send to recipient's active user socket room if online
                io.to(`user:${recipient}`).emit('receiveMessage', messagePayload);
                
                // Echo back to sender for device synchronization
                io.to(`user:${socket.username}`).emit('receiveMessage', messagePayload);
            }
        });
    });

    // --- MESSAGE REACTION SYSTEM ---
    socket.on('addReaction', ({ msgId, emoji, target, isGroup }) => {
        if (!socket.username || !msgId || !emoji) return;

        db.get(`SELECT reactions FROM messages WHERE id = ?`, [msgId], (err, row) => {
            if (err || !row) return;

            let reactions = {};
            try {
                reactions = JSON.parse(row.reactions || '{}');
            } catch (e) {
                reactions = {};
            }

            // Toggle reaction: if exists with same emoji, remove it; else set it
            if (reactions[socket.username] === emoji) {
                delete reactions[socket.username];
            } else {
                reactions[socket.username] = emoji;
            }

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
        });
    });

    // --- TYPING INDICATORS ---
    socket.on('typing', ({ target, isGroup, isTyping }) => {
        if (!socket.username || !target) return;

        if (isGroup) {
            socket.to(target).emit('userTyping', { from: socket.username, room: target, isTyping });
        } else {
            io.to(`user:${target}`).emit('userTyping', { from: socket.username, isTyping });
        }
    });

    // --- READ RECEIPTS ACKNOWLEDGEMENT ---
    socket.on('markAsRead', ({ sender }) => {
        if (!socket.username || !sender) return;

        db.run(`UPDATE messages SET status = 'read' WHERE sender = ? AND recipient = ? AND status != 'read'`, [sender, socket.username], () => {
            io.to(`user:${sender}`).emit('messagesMarkedRead', { by: socket.username });
        });
    });

    // --- WEBRTC CALL NOTIFICATION SIGNALING ---
    socket.on('startCallNotification', ({ target, isGroup, callRoomName, isAudioOnly }) => {
        if (!socket.username || !target) return;

        const payload = {
            from: socket.username,
            callRoomName,
            isAudioOnly,
            isGroup: !!isGroup,
            groupName: isGroup ? target : null
        };

        if (isGroup) {
            socket.to(target).emit('incomingCall', payload);
        } else {
            io.to(`user:${target}`).emit('incomingCall', payload);
        }
    });

    socket.on('declineCall', ({ to }) => {
        if (!socket.username || !to) return;
        io.to(`user:${to}`).emit('callDeclined', { from: socket.username });
    });

    // --- DISCONNECT HANDLING ---
    socket.on('disconnect', () => {
        console.log(`[Socket Disconnected]: ${socket.id}`);
        if (socket.username && onlineUsers.has(socket.username)) {
            const userSockets = onlineUsers.get(socket.username);
            userSockets.delete(socket.id);

            if (userSockets.size === 0) {
                onlineUsers.delete(socket.username);
                broadcastUserDirectory();
            }
        }
    });
});

// Start Server Listening
const PORT = process.env.PORT || 3000;
http.listen(PORT, () => {
    console.log(`=======================================================`);
    console.log(`  WhatsApp Chatz Enterprise Server Running on Port ${PORT} `);
    console.log(`  URL: http://localhost:${PORT}                           `);
    console.log(`=======================================================`);
});