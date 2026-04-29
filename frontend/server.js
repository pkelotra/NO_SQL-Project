// server.js
const express = require('express');
const multer = require('multer');
const path = require('path');
const cors = require('cors');

const app = express();
const PORT = 3000;

// Enable CORS and serve static files from the 'public' directory
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// Configure Multer for file uploads
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, 'uploads/');
    },
    filename: (req, file, cb) => {
        cb(null, Date.now() + '-' + file.originalname);
    }
});
const upload = multer({ storage: storage });

// API Endpoint to handle pipeline execution
app.post('/api/run-pipeline', upload.single('logfile'), (req, res) => {
    try {
        const file = req.file;
        const method = req.body.method;
        const batchSize = req.body.batchSize;

        if (!file) {
            return res.status(400).json({ error: 'No log file uploaded.' });
        }

        console.log(`[PIPELINE] Triggered: ${method}`);
        console.log(`[PIPELINE] Batch Size: ${batchSize}`);
        console.log(`[PIPELINE] File Path: ${file.path}`);

        // TODO FOR YOUR FRIEND: Replace this setTimeout block with actual child_process.exec()
        // Example: exec(`hadoop jar ... ${file.path}`, (error, stdout, stderr) => { ... })
        
        // Simulating processing time
        setTimeout(() => {
            res.json({
                success: true,
                message: 'Pipeline executed successfully.',
                details: {
                    methodRun: method,
                    batchSizeUsed: batchSize,
                    fileProcessed: file.originalname,
                    executionTimeMs: Math.floor(Math.random() * 5000) + 1000,
                    status: 'COMPLETED'
                }
            });
        }, 2000);

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'An error occurred during pipeline execution.' });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`);
});
