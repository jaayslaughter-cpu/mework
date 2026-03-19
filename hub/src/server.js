// hub/src/server.js
// PropIQ Analytics Fast-Data Hub

const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors({
  origin: ['http://localhost:8501', 'http://localhost:3000']
}));
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

const PORT = process.env.PORT || 3002;

app.listen(PORT, () => {
  console.log(`⚾ PropIQ Hub listening on :${PORT}`);
});
