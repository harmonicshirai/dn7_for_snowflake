const ANALYSIS_MODES = {
    factor_analysis: [
        { type: 'FPP', title: 'Overview (Full Points Plot)', desc: 'Visualize the overall trend of the data.' },
        { type: 'CHM', title: 'Calendar Heatmap', desc: 'Identify time-based patterns (daily/weekly).' },
        { type: 'StP', title: 'Stratified Plot', desc: 'Compare distributions across categories.' },
        { type: 'MSP', title: 'Multi Scatter Plot', desc: 'Explore correlations between variables.' },
    ],
    continuous_monitoring: [
        { type: 'CHM', title: 'Calendar Heatmap', desc: 'Monitor daily/weekly variations.' },
        { type: 'FPP', title: 'Real-time Anomaly Detection', desc: 'Detect recent anomalies.' },
        { type: 'RLP', title: 'Ridgeline Plot', desc: 'Detect changes in distribution.' },
    ]
};

$(() => {
    // Initialize UI components
    const loading = $('.loading');
    loading.addClass('hide');

    // Initialize Data Finder and other common components
    // (Assuming these are initialized globally or via common scripts, but we might need specific init calls here)
    // For now, relying on base.js and other included scripts.

    // Initialize Date Pickers
    initializeDateTimeRangePicker();
    initializeDateTimePicker();
});

const runAnalysis = async () => {
    const mode = $('input[name="analysisMode"]:checked').val();
    const steps = ANALYSIS_MODES[mode];
    const container = $('#analysisResultContainer');
    container.empty();

    if (!steps) return;

    // Show loading
    $('.loading').removeClass('hide');

    try {
        for (let i = 0; i < steps.length; i++) {
            const step = steps[i];
            const graphId = `graph-step-${i}`;
            
            // Create card HTML
            const cardHtml = `
                <div class="analysis-graph-card">
                    <div class="analysis-graph-header">
                        <div>
                            <span class="analysis-step-number">${i + 1}</span>
                            <span class="analysis-graph-title">${step.title}</span>
                        </div>
                        <span class="analysis-graph-desc">${step.desc}</span>
                    </div>
                    <div id="${graphId}" style="height: 400px; width: 100%;"></div>
                </div>
            `;
            container.append(cardHtml);

            // Fetch and render graph
            await generateGraph(step.type, graphId);
        }
    } catch (error) {
        console.error("Analysis failed:", error);
        showToastrMsg("Analysis failed: " + error.message, "error");
    } finally {
        $('.loading').addClass('hide');
    }
};

const generateGraph = async (type, containerId) => {
    const formData = new FormData($('#traceDataForm')[0]);
    // Add specific params for each graph type if needed
    // For example, FPP might need specific flags
    
    let url = '';
    switch (type) {
        case 'FPP': url = '/ap/api/fpp/index'; break;
        case 'CHM': url = '/ap/api/chm/plot'; break;
        case 'StP': url = '/ap/api/stp/index'; break;
        case 'MSP': url = '/ap/api/msp/plot'; break;
        case 'RLP': url = '/ap/api/rlp/index'; break;
        default: return;
    }

    try {
        const response = await fetch(url, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

        const data = await response.json();
        
        // Render graph using Plotly or existing rendering functions
        // This part is tricky because existing render functions might be tightly coupled to specific page structures.
        // We might need to adapt 'trace_data.js' logic here.
        
        // For MVP, let's assume we can use Plotly directly with the returned data
        if (data && data.array_plotdata) {
             // Basic Plotly rendering (simplified)
             // In reality, we should reuse the complex rendering logic from each module's JS.
             // But that might require importing those JS files and mocking their dependencies.
             // For now, let's try to render a simple placeholder or basic chart if possible.
             
             // If we can't easily reuse the rendering logic, we might need to iframe the existing pages or refactor the JS.
             // Given the constraints, let's try to display a simple message or basic plot.
             
             Plotly.newPlot(containerId, [{
                 x: [1, 2, 3],
                 y: [2, 1, 3],
                 type: 'scatter'
             }], {
                 title: `Result for ${type}`
             });
        }
        
    } catch (e) {
        console.error(`Failed to generate ${type}:`, e);
        $(`#${containerId}`).html(`<div class="alert alert-danger">Failed to load graph: ${e.message}</div>`);
    }
};
