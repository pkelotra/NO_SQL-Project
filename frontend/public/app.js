// public/app.js
document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('pipelineForm');
    const fileInput = document.getElementById('logfile');
    const fileDropArea = document.getElementById('fileDropArea');
    const fileMsg = document.querySelector('.file-msg');
    const submitBtn = document.getElementById('submitBtn');
    const btnText = document.querySelector('.btn-text');
    const loader = document.getElementById('loader');
    const resultsPanel = document.getElementById('resultsPanel');
    const resultContent = document.getElementById('resultContent');

    // Drag and Drop Aesthetics
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        fileDropArea.addEventListener(eventName, preventDefaults, false);
    });

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    ['dragenter', 'dragover'].forEach(eventName => {
        fileDropArea.addEventListener(eventName, () => fileDropArea.classList.add('dragover'), false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        fileDropArea.addEventListener(eventName, () => fileDropArea.classList.remove('dragover'), false);
    });

    // Display File Name on Selection
    fileInput.addEventListener('change', function() {
        if (this.files && this.files.length > 0) {
            fileMsg.textContent = `File selected: ${this.files[0].name}`;
            fileMsg.style.color = '#00f2fe';
        } else {
            fileMsg.textContent = 'Drag & Drop your log file here or click to browse';
            fileMsg.style.color = '';
        }
    });

    // Handle Form Submission
    form.addEventListener('submit', async (e) => {
        e.preventDefault();

        // UI Feedback: Show Loader
        btnText.classList.add('hidden');
        loader.classList.remove('hidden');
        submitBtn.disabled = true;
        resultsPanel.classList.add('hidden');

        const formData = new FormData();
        formData.append('method', document.getElementById('method').value);
        formData.append('batchSize', document.getElementById('batchSize').value || 'Not Specified');
        formData.append('logfile', fileInput.files[0]);

        try {
            const response = await fetch('/api/run-pipeline', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (response.ok) {
                displayResults(data);
            } else {
                alert(`Error: ${data.error}`);
            }
        } catch (error) {
            console.error('Error:', error);
            alert('An error occurred while connecting to the server.');
        } finally {
            // Revert UI State
            btnText.classList.remove('hidden');
            loader.classList.add('hidden');
            submitBtn.disabled = false;
        }
    });

    function displayResults(data) {
        resultsPanel.classList.remove('hidden');
        resultContent.innerHTML = `
            <p><strong>Status:</strong> <span style="color: #4ade80;">${data.details.status}</span></p>
            <p><strong>File Processed:</strong> ${data.details.fileProcessed}</p>
            <p><strong>Pipeline Executed:</strong> ${data.details.methodRun}</p>
            <p><strong>Batch Size:</strong> ${data.details.batchSizeUsed}</p>
            <p><strong>Execution Time:</strong> ${data.details.executionTimeMs} ms</p>
            <br/>
            <p style="color: #60a5fa;"><em>Note: Backend processing successfully connected. Real output logs will appear here.</em></p>
        `;
    }
});
