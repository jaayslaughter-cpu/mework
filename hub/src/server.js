// hub/src/server.js
// PropIQ Analytics Fast-Data Hub

const express = require('express');
const cors = require('cors');
const slatesRouter = require('./routes/slates');

const app = express();
app.use(cors({
  origin: ['http://localhost:8501', 'http://localhost:3000']
}));
app.use(express.json());

// Register routes
app.use('/api/slates', slatesRouter);

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), timestamp: Date.now() });
});

const PORT = process.env.PORT || 3002;

app.listen(PORT, () => {
  console.log(`⚾ PropIQ Hub listening on :${PORT}`);
});

// Start the background sync worker ONLY if explicitly enabled
if (process.env.ENABLE_SYNC_WORKER === 'true') {
  const { startSyncWorker } = require('./sync');
  startSyncWorker();
}
