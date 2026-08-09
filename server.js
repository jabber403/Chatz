const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http, {
    maxHttpBufferSize: 1e8, // 100MB max payload for files and media
    cors: { 
        origin: "*", 
        methods: ["GET", "POST"] 
    },
    pingInterval: 10000,
    pingTimeout: 5000
});
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const crypto = require('crypto');

// Initialize SQLite Database with Write-Ahead Logging (WAL) for optimal concurrency
const db = new sqlite3.Database('./chat.db', (err) => {
    if (err) {
        console.error('Failed to connect to SQLite database:', err.message);
    } else {
        console.log('Connected to SQLite database successfully.');
    }
});

db.run('PRAGMA journal_mode = WAL;');

// Database Schema Initialization with Safe Migration Support
db.serialize(() => {
    // Base Users Table
    db.run(`CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, 
        password TEXT NOT NULL, 
        pfp TEXT DEFAULT '👤'
    )`);

    // Safely add missing columns for pre-existing chat.db files
    db.run(`ALTER TABLE users ADD COLUMN session_token TEXT`, () => {});
    db.run(`ALTER TABLE users ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP`, () => {});

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

    // User blocking relationship table
    db.run(`CREATE TABLE IF NOT EXISTS blocked_users (
        blocker TEXT NOT NULL,
        blocked TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (blocker, blocked)
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
 * Helper function to generate secure unique session tokens
 */
function generateToken() {
    return crypto.randomBytes(32).toString('hex');
}

/**
 * Checks whether userA has blocked userB or vice-versa
 */
function isBlockedRelationship(userA, userB, callback) {
    if (!userA || !userB) return callback(false);
    const query = `
        SELECT 1 FROM blocked_users 
        WHERE (blocker = ? AND blocked = ?) OR (blocker = ? AND blocked = ?)
        LIMIT 1
    `;
    db.get(query, [userA, userB, userB, userA], (err, row) => {
        callback(!!row);
    });
}

/**
 * Retrieves the full list of users blocked by a specific username
 */
function getBlockedList(username, callback) {
    db.all('SELECT blocked FROM blocked_users WHERE blocker = ?', [username], (err, rows) => {
        if (err || !rows) return callback([]);
        callback(rows.map(r => r.blocked));
    });
}

/**
 * Broadcasts full user directory with online/offline indicators to connected clients
 */
function broadcastUserDirectory() {
    db.all('SELECT username, pfp FROM users', [], (err, rows) => {
        if (err || !rows) return;

        io.sockets.sockets.forEach((targetSocket) => {
            if (!targetSocket.username) return;

            getBlockedList(targetSocket.username, (blockedByMe) => {
                db.all('SELECT blocker FROM blocked_users WHERE blocked = ?', [targetSocket.username], (err2, blockedMeRows) => {
                    const blockedMe = blockedMeRows ? blockedMeRows.map(r => r.blocker) : [];

                    const userList = rows.map(u => {
                        const isBlockedBySelf = blockedByMe.includes(u.username);
                        const hasBlockedSelf = blockedMe.includes(u.username);
                        const isOnline = onlineUsers.has(u.username) && onlineUsers.get(u.username).size > 0;

                        return {
                            username: u.username,
                            pfp: u.pfp || '👤',
                            online: hasBlockedSelf ? false : isOnline,
                            isBlocked: isBlockedBySelf
                        };
                    });

                    targetSocket.emit('updateUserList', userList);
                });
            });
        });
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

    // --- AUTOMATIC SESSION LOGIN (TOKEN BASED) ---
    socket.on('autoLogin', ({ sessionToken }) => {
        if (!sessionToken) {
            return socket.emit('autoLoginResponse', { success: false });
        }

        db.get('SELECT username, pfp FROM users WHERE session_token = ?', [sessionToken], (err, row) => {
            if (err || !row) {
                return socket.emit('autoLoginResponse', { success: false });
            }
            completeUserSession(row.username, row.pfp || '👤', sessionToken, true);
        });
    });

    // --- STANDARD USER AUTHENTICATION & LOGIN ---
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

            const newToken = generateToken();

            if (row) {
                // Existing User Authentication
                if (row.password === password) {
                    const finalPfp = (pfp && pfp !== '👤') ? pfp : (row.pfp || '👤');
                    db.run('UPDATE users SET pfp = ?, session_token = ? WHERE username = ?', [finalPfp, newToken, trimmedUser], () => {
                        completeUserSession(trimmedUser, finalPfp, newToken, false);
                    });
                } else {
                    socket.emit('loginResponse', { success: false, msg: 'Invalid password!' });
                }
            } else {
                // New User Registration
                const initialPfp = pfp || '👤';
                db.run('INSERT INTO users (username, password, pfp, session_token) VALUES (?, ?, ?, ?)', [trimmedUser, password, initialPfp, newToken], (err) => {
                    if (err) {
                        return socket.emit('loginResponse', { success: false, msg: 'Could not create account.' });
                    }
                    completeUserSession(trimmedUser, initialPfp, newToken, false);
                });
            }
        });
    });

    function completeUserSession(user, userPfp, token, isAuto) {
        socket.username = user;
        socket.pfp = userPfp;

        if (!onlineUsers.has(user)) {
            onlineUsers.set(user, new Set());
        }
        onlineUsers.get(user).add(socket.id);
        socket.join(`user:${user}`);

        db.all('SELECT room FROM group_members WHERE username = ?', [user], (err, rows) => {
            const groups = rows ? rows.map(r => r.room) : [];
            groups.forEach(g => socket.join(g));

            db.run(`UPDATE messages SET status = 'delivered' WHERE recipient = ? AND status = 'sent'`, [user], () => {
                fetchUnreadCounts(user, (unreadMap) => {
                    getBlockedList(user, (blockedList) => {
                        const responsePayload = {
                            success: true,
                            user: { username: user, pfp: userPfp, sessionToken: token },
                            groups,
                            unreadMap,
                            blockedList
                        };

                        if (isAuto) {
                            socket.emit('autoLoginResponse', responsePayload);
                        } else {
                            socket.emit('loginResponse', responsePayload);
                        }
                        broadcastUserDirectory();
                    });
                });
            });
        });
    }

    // --- USER BLOCKING & UNBLOCKING ---
    socket.on('blockUser', (targetUser) => {
        if (!socket.username || !targetUser || socket.username === targetUser) return;

        db.run('INSERT OR IGNORE INTO blocked_users (blocker, blocked) VALUES (?, ?)', [socket.username, targetUser], () => {
            getBlockedList(socket.username, (blockedList) => {
                socket.emit('blockedListUpdated', blockedList);
                broadcastUserDirectory();
            });
        });
    });

    socket.on('unblockUser', (targetUser) => {
        if (!socket.username || !targetUser) return;

        db.run('DELETE FROM blocked_users WHERE blocker = ? AND blocked = ?', [socket.username, targetUser], () => {
            getBlockedList(socket.username, (blockedList) => {
                socket.emit('blockedListUpdated', blockedList);
                broadcastUserDirectory();
            });
        });
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
            isBlockedRelationship(socket.username, target, (blocked) => {
                const query = `
                    SELECT id, sender, recipient, room, message, fileData, fileName, fileType, status, reactions, strftime('%H:%M', timestamp, 'localtime') as time 
                    FROM messages 
                    WHERE (sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?) 
                    ORDER BY id ASC
                `;
                db.all(query, [socket.username, target, target, socket.username], (err, rows) => {
                    if (err) return;

                    db.run(`UPDATE messages SET status = 'read' WHERE sender = ? AND recipient = ? AND status != 'read'`, [target, socket.username], () => {
                        io.to(`user:${target}`).emit('messagesMarkedRead', { by: socket.username });
                        socket.emit('conversationHistory', { target, isGroup: false, history: rows || [], isBlocked: blocked });
                    });
                });
            });
        }
    });

    // --- REAL-TIME & OFFLINE MESSAGE TRANSMISSION ---
    socket.on('sendMessage', (data) => {
        if (!socket.username) return;

        const recipient = data.recipient || null;
        const room = data.room || null;

        if (recipient) {
            isBlockedRelationship(socket.username, recipient, (blocked) => {
                if (blocked) {
                    return socket.emit('systemNotification', { text: 'Message not sent. Blocking interaction in effect.' });
                }
                processMessagePersistence();
            });
        } else {
            processMessagePersistence();
        }

        function processMessagePersistence() {
            const now = new Date();
            const timeStr = now.getHours().toString().padStart(2, '0') + ':' + now.getMinutes().toString().padStart(2, '0');

            const isRecipientOnline = recipient && onlineUsers.has(recipient) && onlineUsers.get(recipient).size > 0;
            let initialStatus = isRecipientOnline ? 'delivered' : 'sent';

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
                    io.to(room).emit('receiveMessage', messagePayload);
                } else if (recipient) {
                    io.to(`user:${recipient}`).emit('receiveMessage', messagePayload);
                    io.to(`user:${socket.username}`).emit('receiveMessage', messagePayload);
                }
            });
        }
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
            isBlockedRelationship(socket.username, target, (blocked) => {
                if (!blocked) {
                    io.to(`user:${target}`).emit('userTyping', { from: socket.username, isTyping });
                }
            });
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
            isBlockedRelationship(socket.username, target, (blocked) => {
                if (!blocked) {
                    io.to(`user:${target}`).emit('incomingCall', payload);
                }
            });
        }
    });

    socket.on('declineCall', ({ to }) => {
        if (!socket.username || !to) return;
        io.to(`user:${to}`).emit('callDeclined', { from: socket.username });
    });

    // --- LOGOUT ENGINE ---
    socket.on('logout', () => {
        if (socket.username) {
            db.run('UPDATE users SET session_token = NULL WHERE username = ?', [socket.username], () => {
                if (onlineUsers.has(socket.username)) {
                    onlineUsers.get(socket.username).delete(socket.id);
                    if (onlineUsers.get(socket.username).size === 0) {
                        onlineUsers.delete(socket.username);
                    }
                }
                socket.username = null;
                socket.pfp = null;
                socket.emit('logoutComplete');
                broadcastUserDirectory();
            });
        }
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