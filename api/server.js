const express = require('express');
const path = require('path');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    maxHttpBufferSize: 1e7 // Increase buffer size to 10MB to handle in-memory file transfers smoothly
});

app.use(express.json());
// Replace app.use(express.static(...)) with this:
app.use(express.static(path.join(__dirname, '../public')));

// Persistent Localized Memory Database Storage
let users = [
    { username: "archit", email: "archit@gmail.com", password: "password123" }
];
let activeMeetings = {}; // Structure: { meetingId: [ { socketId, username, peerId } ] }

// --- AUTHENTICATION API MATRIX ---

app.post('/api/register', (req, res) => {
    const { username, email, password } = req.body;
    if (!username || !email || !password) return res.status(400).json({ success: false, message: "All fields are required." });
    
    const formattedUser = username.toLowerCase().trim();
    if (users.find(u => u.username === formattedUser || u.email === email)) {
        return res.status(400).json({ success: false, message: "Username or Email already exists." });
    }

    users.push({ username: formattedUser, email, password });
    return res.status(201).json({ success: true });
});

app.post('/api/login', (req, res) => {
    const { username, password } = req.body;
    const formattedUser = username.toLowerCase().trim();
    const user = users.find(u => u.username === formattedUser && u.password === password);

    if (!user) return res.status(400).json({ success: false, message: "Invalid credentials." });
    return res.json({ success: true, user: { username: user.username, email: user.email } });
});

// --- REAL-TIME COMMUNICATION ENGINE (SOCKET.IO) ---

io.on('connection', (socket) => {
    let currentRoom = null;
    let myUsername = null;

    // Join Meeting Room Context Pipeline
    socket.on('join-room', ({ roomCode, username, peerId }) => {
        currentRoom = roomCode;
        myUsername = username;
        socket.join(roomCode);

        if (!activeMeetings[roomCode]) activeMeetings[roomCode] = [];
        
        // Push participant identity signature parameters to meeting matrix
        activeMeetings[roomCode].push({ socketId: socket.id, username, peerId });
        
        // Notify other peers inside room to establish fresh streaming mesh links
        socket.to(roomCode).emit('user-connected', { socketId: socket.id, username, peerId });
        io.to(roomCode).emit('update-users-list', activeMeetings[roomCode]);
    });

    // WebRTC Signaling Forwarding Matrices
    socket.on('signal-offer', ({ targetSocketId, offer }) => {
        socket.to(targetSocketId).emit('signal-offer', { senderSocketId: socket.id, offer });
    });

    socket.on('signal-answer', ({ targetSocketId, answer }) => {
        socket.to(targetSocketId).emit('signal-answer', { senderSocketId: socket.id, answer });
    });

    socket.on('signal-ice', ({ targetSocketId, candidate }) => {
        socket.to(targetSocketId).emit('signal-ice', { senderSocketId: socket.id, candidate });
    });

    // Synchronized Collaborative Whiteboard Actions
    socket.on('whiteboard-draw', (drawData) => {
        if (currentRoom) socket.to(currentRoom).emit('whiteboard-draw', drawData);
    });

    socket.on('whiteboard-clear', () => {
        if (currentRoom) socket.to(currentRoom).emit('whiteboard-clear');
    });

    // Real-Time Encrypted Chat Text Actions
    socket.on('chat-message', (msgData) => {
        if (currentRoom) io.to(currentRoom).emit('chat-message', { ...msgData, timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) });
    });

    // Disconnect Cleanup Wrapper State Machine
    socket.on('disconnect', () => {
        if (currentRoom && activeMeetings[currentRoom]) {
            activeMeetings[currentRoom] = activeMeetings[currentRoom].filter(u => u.socketId !== socket.id);
            socket.to(currentRoom).emit('user-disconnected', { socketId: socket.id, username: myUsername });
            io.to(currentRoom).emit('update-users-list', activeMeetings[currentRoom]);

            if (activeMeetings[currentRoom].length === 0) delete activeMeetings[currentRoom];
        }
    });
});

// Replace your fallback middleware with this:
app.use((req, res) => {
    res.sendFile(path.join(__dirname, '../public/index.html'));
});
const PORT = process.env.PORT || 3000;
// server.listen(PORT, () => console.log(`Arch-meet framework running at http://localhost:${PORT}`));
// Add this at the absolute bottom
module.exports = app;