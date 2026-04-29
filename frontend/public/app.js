// public/app.js
document.addEventListener('DOMContentLoaded', () => {

    // --- Element References ---
    const form            = document.getElementById('pipelineForm');
    const fileInput       = document.getElementById('logfile');
    const fileDropArea    = document.getElementById('fileDropArea');
    const dropBadge       = document.getElementById('dropBadge');
    const submitBtn       = document.getElementById('submitBtn');
    const btnText         = document.getElementById('btnText');
    const loader          = document.getElementById('loader');
    const resultsPanel    = document.getElementById('resultsPanel');
    const resultContent   = document.getElementById('resultContent');
    const datasetSelect   = document.getElementById('datasetName');
    const datasetModeInput = document.getElementById('datasetModeInput');

    const togglePresplit  = document.getElementById('togglePresplit');
    const toggleCustom    = document.getElementById('toggleCustom');
    const preSplitSection = document.getElementById('preSplitSection');
    const customSection   = document.getElementById('customFileSection');

    const btnViewAnalysis = document.getElementById('btnViewAnalysis');
    const btnReset        = document.getElementById('btnReset');
    const analysisContainer = document.getElementById('analysisContainer');
    const qualityChart    = document.getElementById('qualityChart');

    let currentExecutionId = null;
    let pollInterval = null;

    // --- Fetch Available Datasets ---
    fetch('/api/datasets')
        .then(res => res.json())
        .then(data => {
            datasetSelect.innerHTML = '<option value="" disabled selected>Select a dataset...</option>';
            if (data.datasets && data.datasets.length > 0) {
                data.datasets.forEach(ds => {
                    const opt = document.createElement('option');
                    opt.value = ds;
                    opt.textContent = `📁 ${ds}`;
                    datasetSelect.appendChild(opt);
                });
            } else {
                datasetSelect.innerHTML = '<option value="" disabled selected>No datasets found</option>';
            }
        });

    // --- Toggle Dataset Mode ---
    function setMode(mode) {
        datasetModeInput.value = mode;
        if (mode === 'pre_split') {
            togglePresplit.classList.add('active');
            toggleCustom.classList.remove('active');
            preSplitSection.classList.remove('fade-hidden');
            customSection.classList.add('fade-hidden');
        } else {
            toggleCustom.classList.add('active');
            togglePresplit.classList.remove('active');
            customSection.classList.remove('fade-hidden');
            preSplitSection.classList.add('fade-hidden');
        }
    }

    togglePresplit.addEventListener('click', () => setMode('pre_split'));
    toggleCustom.addEventListener('click',   () => setMode('custom'));

    // --- Drag & Drop ---
    fileInput.addEventListener('change', () => {
        if (fileInput.files && fileInput.files.length > 0) {
            dropBadge.textContent = `✓ ${fileInput.files[0].name}`;
            dropBadge.classList.add('has-file');
        }
    });

    // --- Form Submission ---
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const mode = datasetModeInput.value;
        const pipeline = document.querySelector('input[name="pipelineEngine"]:checked');

        if (!pipeline) return showToast('Select an engine!');
        
        btnText.classList.add('hidden');
        loader.classList.remove('hidden');
        submitBtn.disabled = true;
        resultsPanel.classList.add('hidden');
        analysisContainer.classList.add('hidden');

        const formData = new FormData();
        formData.append('method', pipeline.value);
        formData.append('datasetMode', mode);
        if (mode === 'pre_split') formData.append('datasetName', datasetSelect.value);
        else formData.append('logfile', fileInput.files[0]);

        try {
            const response = await fetch('/api/run-pipeline', { method: 'POST', body: formData });
            const data = await response.json();
            showToast(data.message);
            
            // Start Polling for completion
            startStatusPolling();
        } catch {
            showToast('Connection error!');
            resetBtn();
        }
    });

    function startStatusPolling() {
        if (pollInterval) clearInterval(pollInterval);
        pollInterval = setInterval(async () => {
            try {
                const res = await fetch('/api/pipeline-status');
                const data = await res.json();
                
                if (data.status === 'COMPLETED_PIPELINE') {
                    clearInterval(pollInterval);
                    currentExecutionId = data.id;
                    displayPipelineFinished(data.id);
                    resetBtn();
                }
            } catch (e) {}
        }, 2000);
    }

    function displayPipelineFinished(id) {
        resultsPanel.classList.remove('hidden');
        resultContent.innerHTML = `
            <div class="result-item result-wide">
                <div class="result-key">LATEST EXECUTION SUCCESSFUL</div>
                <div class="result-val" style="color:var(--accent-green)">Execution ID: ${id}</div>
            </div>
            <div class="result-item result-wide">
                <div class="result-key">STATUS</div>
                <div class="result-val">Pipeline finished in terminal. Click View Analysis to see the Data Quality chart.</div>
            </div>
        `;
        resultsPanel.scrollIntoView({ behavior: 'smooth' });
    }

    // --- View Analysis ---
    btnViewAnalysis.addEventListener('click', async () => {
        if (!currentExecutionId) return;
        
        btnViewAnalysis.textContent = "⏳ Analyzing...";
        btnViewAnalysis.disabled = true;

        try {
            // 1. Trigger the Master Analyzer
            await fetch('/api/run-analysis', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id: currentExecutionId })
            });

            // 2. Poll for Analysis completion
            let analysisPoll = setInterval(async () => {
                const res = await fetch('/api/pipeline-status');
                const data = await res.json();
                if (data.status === 'COMPLETED_ANALYSIS') {
                    clearInterval(analysisPoll);
                    showAnalysisChart(currentExecutionId);
                    btnViewAnalysis.textContent = "📊 View Analysis";
                    btnViewAnalysis.disabled = false;
                }
            }, 1000);

        } catch (e) {
            showToast('Analysis failed!');
            btnViewAnalysis.textContent = "📊 View Analysis";
            btnViewAnalysis.disabled = false;
        }
    });

    function showAnalysisChart(id) {
        analysisContainer.classList.remove('hidden');
        // Add a timestamp to bypass browser cache
        qualityChart.src = `/graphs/DataQuality/quality_chart_${id}.png?t=${Date.now()}`;
        analysisContainer.scrollIntoView({ behavior: 'smooth' });
    }

    btnReset.addEventListener('click', () => {
        resultsPanel.classList.add('hidden');
        window.scrollTo({ top: 0, behavior: 'smooth' });
    });

    function resetBtn() {
        btnText.classList.remove('hidden');
        loader.classList.add('hidden');
        submitBtn.disabled = false;
    }

    function showToast(msg) {
        let toast = document.querySelector('.toast');
        if (!toast) {
            toast = document.createElement('div');
            toast.className = 'toast';
            document.body.appendChild(toast);
        }
        toast.textContent = msg;
        toast.classList.add('visible');
        setTimeout(() => toast.classList.remove('visible'), 3500);
    }
});
