// ============================================================
// app.js — Portal de transparencia · Rincón de la Victoria
// SPA vanilla JS sin build step. Lee JSONs desde docs/data/.
//
// Estructura:
//   1. Estado global
//   2. Inicialización
//   3. Renderizado (dashboard, gráficos, tabla, contratistas, calidad)
//   4. Filtros y búsqueda
//   5. Utilidades (formato, CSV)
// ============================================================


// ── 1. Estado global ─────────────────────────────────────────
let allContracts = [];
let analysisData = {};
let auditData = {};
let contractorsSummary = [];
let filteredContracts = [];
let filteredContractors = [];
let currentPage = 1;
const ITEMS_PER_PAGE = 20;

let sortKey = 'fecha_adjudicacion';
let sortOrder = 'desc';

// Estado de ordenación de mini-tablas por contratista: { cif: { key, order } }
const contractorSortState = {};

// CIFs que ya tienen mini-gráficos renderizados
const contractorChartsRendered = new Set();

// Referencias a gráficos doughnut para actualizar colores al cambiar de tema
let typeChartInstance = null;
let entityChartInstance = null;

function legendColor() {
    return document.documentElement.getAttribute('data-theme') === 'dark'
        ? '#e2e8f0'
        : '#374151';
}

let activeFilters = { anio: '', trimestre: '', area: '', tipo: '', entidad: '', fechaDesde: '', fechaHasta: '', importeMin: '', importeMax: '', texto: '' };


// ── 2. Inicialización ────────────────────────────────────────

async function init() {
    try {
        // ?v=timestamp evita que el navegador sirva JSONs cacheados tras una actualización
        const [contractsRes, analysisRes, summaryRes, auditRes] = await Promise.all([
            fetch('data/contracts.json?v=' + Date.now()),
            fetch('data/analysis.json?v=' + Date.now()),
            fetch('data/contractors_summary.json?v=' + Date.now()),
            fetch('data/audit_summary.json?v=' + Date.now()),
        ]);

        allContracts = await contractsRes.json();
        analysisData = await analysisRes.json();
        contractorsSummary = await summaryRes.json();
        auditData = await auditRes.json();

        filteredContracts = [...allContracts];
        filteredContractors = [...contractorsSummary];

        sortData();
        renderDashboard();
        // renderCharts() se llama de forma lazy al activar la pestaña Análisis
        renderTable();
        renderContractors();
        renderQuality();
        populateFilters();
        setupSearch();
        setupSorting();
        setupTabs();
        setupFiltersToggle();
        setupThemeToggle();
        setupAboutToggle();
        displayLastUpdated();
        updateResultsCount();
        lucide.createIcons();
    } catch (err) {
        console.error("Error loading data:", err);
    }
}

function displayLastUpdated() {
    const el = document.getElementById('lastUpdated');
    if (el && analysisData.summary?.last_updated) {
        el.textContent = `Datos actualizados a ${analysisData.summary.last_updated}`;
    }
}

function setupThemeToggle() {
    const btn = document.getElementById('themeToggle');
    if (!btn) return;
    btn.addEventListener('click', () => {
        const current = document.documentElement.getAttribute('data-theme');
        const isDark = current === 'dark';
        const next = isDark ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', next);
        localStorage.setItem('theme', next);
        // Actualizar color de leyenda en gráficos doughnut
        const color = legendColor();
        [typeChartInstance, entityChartInstance].forEach(ch => {
            if (!ch) return;
            ch.options.plugins.legend.labels.color = color;
            ch.update();
        });
    });
}

function setupAboutToggle() {
    const section = document.getElementById('about');
    if (!section) return;
    const toggle = () => section.classList.toggle('about-collapsed');
    // Clic en el h2 de la sección
    const h2 = section.querySelector('h2');
    if (h2) h2.addEventListener('click', toggle);
    // Clic en cualquier enlace que apunte a #about (header + footer)
    document.querySelectorAll('a[href="#about"]').forEach(link => {
        link.addEventListener('click', e => {
            e.preventDefault();
            toggle();
            section.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
    });
}

function setupFiltersToggle() {
    const btn   = document.getElementById('filtersToggle');
    const panel = document.getElementById('filtersPanel');
    if (!btn || !panel) return;
    btn.addEventListener('click', () => {
        panel.classList.toggle('open');
        btn.classList.toggle('active');
    });
}

let chartsRendered = false;

function setupTabs() {
    const tabs = document.querySelectorAll('.tab-btn');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const view = tab.getAttribute('data-view');
            if (!view) return;

            // UI Update
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');

            document.querySelectorAll('.view-content').forEach(content => {
                content.classList.remove('active');
            });
            document.getElementById(`${view}View`).classList.add('active');

            // Lazy: renderizar gráficos solo la primera vez que se abre la pestaña
            // (Chart.js necesita que el canvas sea visible para calcular dimensiones)
            if (view === 'analysis' && !chartsRendered) {
                renderCharts();
                chartsRendered = true;
                lucide.createIcons();
            }
        });
    });
}


// ── 3. Renderizado ───────────────────────────────────────────

// ── Helpers mini-tabla contratistas ──────────────────────────

function sortedContratos(contratos, cif) {
    const { key, order } = contractorSortState[cif] || { key: 'fecha_adjudicacion', order: 'desc' };
    return [...contratos].sort((a, b) => {
        let valA = a[key] ?? null, valB = b[key] ?? null;
        if (valA == null && valB == null) return 0;
        if (valA == null) return 1;
        if (valB == null) return -1;
        if (typeof valA === 'string') {
            const res = valA.localeCompare(valB, 'es', { sensitivity: 'base' });
            return order === 'asc' ? res : -res;
        }
        return order === 'asc' ? valA - valB : valB - valA;
    });
}

function miniTableHeaders(cif) {
    const { key: sk, order: so } = contractorSortState[cif] || { key: 'fecha_adjudicacion', order: 'desc' };
    const ind = k => sk === k ? (so === 'asc' ? ' ↑' : ' ↓') : '';
    const th = (label, k, cls = '') =>
        `<th class="sortable-mini${cls}" onclick="sortContractorBy('${cif}', '${k}')">${label}${ind(k)}</th>`;
    return `<tr>
        ${th('Fecha', 'fecha_adjudicacion')}
        ${th('Objeto', 'objeto')}
        ${th('Área', 'area')}
        ${th('Tipo', 'tipo_contrato_limpio')}
        ${th('Expediente', 'expediente')}
        ${th('Importe', 'importe', ' text-right')}
    </tr>`;
}

function miniTableRows(contratos, cif) {
    return sortedContratos(contratos, cif).map(contract => `
        <tr>
            <td class="text-muted" style="white-space:nowrap">${formatDate(contract.fecha_adjudicacion)}</td>
            <td>${contract.objeto}</td>
            <td class="text-muted">${contract.area || '—'}</td>
            <td>${contract.tipo_contrato_limpio ? `<small class="tipo-tag ${contract.tipo_contrato_limpio.toLowerCase()}">${contract.tipo_contrato_limpio}</small>` : '—'}</td>
            <td class="text-muted">${contract.expediente || '---'}</td>
            <td class="text-right"><strong>${formatCurrency(contract.importe)}</strong></td>
        </tr>
    `).join('');
}

function renderContractorMiniTable(cif) {
    const contractor = filteredContractors.find(c => c.cif === cif);
    if (!contractor) return;
    const card = document.getElementById(`contractor-${cif}`);
    if (!card) return;
    card.querySelector('.mini-contracts-thead').innerHTML = miniTableHeaders(cif);
    card.querySelector('.mini-contracts-tbody').innerHTML = miniTableRows(contractor.contratos, cif);
}

window.sortContractorBy = (cif, key) => {
    const current = contractorSortState[cif] || { key: 'fecha_adjudicacion', order: 'desc' };
    contractorSortState[cif] = {
        key,
        order: current.key === key ? (current.order === 'asc' ? 'desc' : 'asc') : (key === 'importe' ? 'desc' : 'asc'),
    };
    renderContractorMiniTable(cif);
};

// ── Renderizado contratistas ──────────────────────────────────

function renderContractors() {
    const container = document.getElementById('contractorsList');
    container.innerHTML = filteredContractors.map((c, idx) => `
        <div class="contractor-card" id="contractor-${c.cif}">
            <div class="contractor-header" onclick="toggleContractor('${c.cif}')">
                <div class="contractor-main-info">
                    <div class="contractor-rank">#${idx + 1}</div>
                    <div class="contractor-details">
                        <h3>
                            ${c.nombre || 'Desconocido'}
                            ${c.tipo_entidad && c.tipo_entidad !== 'Desconocido' ? `<span class="entity-type type-${getEntityClass(c.tipo_entidad)}" style="margin-left:.5rem;vertical-align:middle">${c.tipo_entidad}</span>` : ''}
                        </h3>
                        <div class="contractor-meta">
                            <span><i data-lucide="hash" style="width:12px"></i> ${c.cif}</span>
                            <span><i data-lucide="map-pin" style="width:12px"></i> ${c.direccion || 'No disponible'}</span>
                            ${c.sector ? `<span class="badge-sector"><i data-lucide="tag" style="width:11px"></i> ${c.sector}</span>` : ''}
                            ${c.fecha_constitucion ? `<span class="badge-meta">${c.fecha_constitucion}</span>` : ''}
                            ${c.estado_registral === 'BAJA' ? `<span class="badge-baja">BAJA</span>` : ''}
                        </div>
                    </div>
                </div>
                <div class="contractor-stats">
                    <div class="total-amount">${formatCurrency(c.total_importe)}</div>
                    <div class="contract-count">${c.num_contratos} contratos</div>
                </div>
                <div style="margin-left: 1.5rem">
                    <i data-lucide="chevron-down" class="chevron-icon"></i>
                </div>
            </div>
            <div class="contractor-expanded">
                <div class="expanded-content">
                    <div class="contractor-mini-charts" id="mc-wrap-${c.cif}">
                        <div class="mini-chart-wrap">
                            <div class="mini-chart-label">Gasto por año</div>
                            <div class="mini-chart-canvas-box"><canvas id="mc-year-${c.cif}"></canvas></div>
                        </div>
                        <div class="mini-chart-wrap">
                            <div class="mini-chart-label">Gasto por área</div>
                            <div class="mini-chart-canvas-box"><canvas id="mc-area-${c.cif}"></canvas></div>
                        </div>
                        <div class="mini-chart-wrap">
                            <div class="mini-chart-label">Gasto por tipo</div>
                            <div class="mini-chart-canvas-box"><canvas id="mc-tipo-${c.cif}"></canvas></div>
                        </div>
                    </div>
                    <table class="mini-contracts-table">
                        <thead class="mini-contracts-thead">${miniTableHeaders(c.cif)}</thead>
                        <tbody class="mini-contracts-tbody">${miniTableRows(c.contratos, c.cif)}</tbody>
                    </table>
                </div>
            </div>
        </div>
    `).join('');
    lucide.createIcons();
}

// Expuesto en window para poder llamarlo desde el onclick inline del HTML
window.toggleContractor = (cif) => {
    const card = document.getElementById(`contractor-${cif}`);
    card.classList.toggle('expanded');
    if (card.classList.contains('expanded') && !contractorChartsRendered.has(cif)) {
        const contractor = filteredContractors.find(c => c.cif === cif);
        if (contractor) renderContractorCharts(cif, contractor.contratos);
    }
};

function renderContractorCharts(cif, contratos) {
    contractorChartsRendered.add(cif);

    const BLUE   = '#3b82f6';
    const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316'];
    const TICK_COLOR = legendColor();  // respeta modo claro/oscuro
    const baseOpts = {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        plugins: { legend: { display: false }, datalabels: { display: false } },
    };

    // Divide un nombre de área en líneas de ≤12 chars (por palabras)
    function wrapAreaLabel(str, maxLen = 12) {
        const words = str.split(' ');
        const lines = [];
        let cur = '';
        for (const w of words) {
            const candidate = cur ? cur + ' ' + w : w;
            if (candidate.length > maxLen && cur) { lines.push(cur); cur = w; }
            else cur = candidate;
        }
        if (cur) lines.push(cur);
        return lines.length > 1 ? lines : str;
    }

    // ── Gráfico por año (línea) ──
    const yearMap = {};
    contratos.forEach(c => {
        const y = c.fecha_adjudicacion ? parseInt(c.fecha_adjudicacion.slice(0, 4), 10) : null;
        if (y && y > 2000) yearMap[y] = (yearMap[y] || 0) + (c.importe || 0);
    });
    const years = Object.keys(yearMap).map(Number).sort((a, b) => a - b);
    if (years.length) {
        const allYears = [];
        for (let y = years[0]; y <= years[years.length - 1]; y++) allYears.push(y);
        const ctxY = document.getElementById(`mc-year-${cif}`);
        if (ctxY) new Chart(ctxY, {
            type: 'line',
            data: {
                labels: allYears.map(String),
                datasets: [{
                    data: allYears.map(y => yearMap[y] || 0),
                    borderColor: BLUE,
                    backgroundColor: 'rgba(59,130,246,0.12)',
                    fill: true,
                    tension: 0.35,
                    pointRadius: 3,
                    pointBackgroundColor: BLUE,
                    borderWidth: 2,
                }]
            },
            options: { ...baseOpts,
                plugins: { ...baseOpts.plugins, tooltip: { callbacks: { label: ctx => formatCurrency(ctx.raw) } } },
                scales: {
                    x: { grid: { display: false }, ticks: { font: { size: 10 }, color: TICK_COLOR } },
                    y: { display: false, beginAtZero: true }
                }
            }
        });
    }

    // ── Gráfico por área (barras + % encima) ──
    const TIPO_COLOR_MAP = {
        'Servicio':   '#2563eb',
        'Suministro': '#7c3aed',
        'Obras':      '#d97706',
        'Otros':      '#64748b',
    };
    const areaMap = {};
    contratos.forEach(c => {
        const a = c.area || 'Sin área';
        areaMap[a] = (areaMap[a] || 0) + (c.importe || 0);
    });
    const areaEntries = Object.entries(areaMap).sort((a, b) => b[1] - a[1]).slice(0, 7);
    const ctxA = document.getElementById(`mc-area-${cif}`);
    if (ctxA && areaEntries.length) {
        const total = areaEntries.reduce((s, e) => s + e[1], 0);
        new Chart(ctxA, {
            type: 'bar',
            plugins: [ChartDataLabels],
            data: {
                labels: areaEntries.map(e => wrapAreaLabel(e[0])),
                datasets: [{
                    data: areaEntries.map(e => e[1]),
                    backgroundColor: 'rgba(37,99,235,0.15)',
                    borderColor: '#2563eb',
                    borderWidth: 1,
                    borderRadius: 3,
                    borderSkipped: false,
                }]
            },
            options: { ...baseOpts,
                layout: { padding: { top: 16 } },
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: ctx => formatCurrency(ctx.raw) } },
                    datalabels: {
                        anchor: 'end', align: 'end', clamp: false,
                        color: TICK_COLOR,
                        font: { size: 9, weight: '600' },
                        formatter: v => total ? Math.round(v / total * 100) + '%' : '',
                    }
                },
                scales: {
                    x: { grid: { display: false }, ticks: { font: { size: 9 }, color: TICK_COLOR, maxRotation: 0 } },
                    y: { display: false, beginAtZero: true }
                }
            }
        });
    }

    // ── Gráfico por tipo (doughnut) ──
    const tipoMap = {};
    contratos.forEach(c => {
        const t = c.tipo_contrato_limpio || c.tipo_contrato || 'Otros';
        tipoMap[t] = (tipoMap[t] || 0) + (c.importe || 0);
    });
    const tipoEntries = Object.entries(tipoMap).sort((a, b) => b[1] - a[1]);
    const ctxT = document.getElementById(`mc-tipo-${cif}`);
    if (ctxT && tipoEntries.length) {
        new Chart(ctxT, {
            type: 'doughnut',
            plugins: [ChartDataLabels],
            data: {
                labels: tipoEntries.map(e => e[0]),
                datasets: [{
                    data: tipoEntries.map(e => e[1]),
                    backgroundColor: tipoEntries.map(e => TIPO_COLOR_MAP[e[0]] || '#64748b'),
                    borderWidth: 0,
                    spacing: 2,
                    hoverOffset: 6,
                }]
            },
            options: { ...baseOpts,
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: ctx => ctx.label + ': ' + formatCurrency(ctx.raw) } },
                    datalabels: {
                        color: '#fff',
                        font: { size: 9, weight: 'bold' },
                        textAlign: 'center',
                        formatter: (v, ctx) => {
                            const tot = ctx.chart.data.datasets[0].data.reduce((a, b) => a + b, 0);
                            const pct = Math.round(v / tot * 100);
                            if (pct < 8) return '';
                            const lbl = ctx.chart.data.labels[ctx.dataIndex];
                            const short = lbl.length > 9 ? lbl.slice(0, 8) + '.' : lbl;
                            return [short, pct + '%'];
                        },
                    },
                },
            }
        });
    }
}

function renderQuality() {
    const { total_checked, by_type = {}, by_year = [] } = auditData;

    const kpis = [
        { label: 'NIF faltante', value: by_type['NIF Faltante'] || 0, icon: 'fingerprint' },
        { label: 'NIF mal formulado', value: by_type['NIF Mal Formulado'] || 0, icon: 'alert-circle' },
        { label: 'Fecha inválida o faltante', value: by_type['Fecha Inválida/Faltante'] || 0, icon: 'calendar-x' },
        { label: 'Importe = 0 €', value: by_type['Importe Cero/Negativo'] || 0, icon: 'circle-off' },
        { label: 'Obra Importe >50K€', value: by_type['Obra Importe >50K€'] || 0, icon: 'triangle-alert' },
        { label: 'Importe >15K€', value: by_type['Importe >15K€'] || 0, icon: 'alert-triangle' },
    ];

    document.getElementById('qualityCards').innerHTML = kpis.map(k => `
        <div class="card stat-card">
            <div style="display:flex;justify-content:space-between;align-items:start">
                <h4>${k.label}</h4>
                <div class="icon-circle"><i data-lucide="${k.icon}" style="width:16px;height:16px"></i></div>
            </div>
            <div class="value ${k.value > 0 ? 'quality-warn' : 'quality-ok'}">${k.value.toLocaleString('es-ES')}</div>
            <div class="quality-pct">${total_checked ? ((k.value / total_checked) * 100).toFixed(1) + '% del total' : ''}</div>
        </div>
    `).join('');

    document.getElementById('qualityTableBody').innerHTML = by_year.map(row => `
        <tr>
            <td><strong>${row.year}</strong></td>
            <td class="text-right">${row.total.toLocaleString('es-ES')}</td>
            <td class="text-right">${formatCurrency(row.importe_total)}</td>
            <td class="text-right ${row.sin_nif > 0 ? 'quality-warn-cell' : ''}">${row.sin_nif}</td>
            <td class="text-right ${row.nif_mal_formulado > 0 ? 'quality-warn-cell' : ''}">${row.nif_mal_formulado}</td>
            <td class="text-right ${row.sin_fecha > 0 ? 'quality-warn-cell' : ''}">${row.sin_fecha}</td>
            <td class="text-right ${row.importe_cero > 0 ? 'quality-warn-cell' : ''}">${row.importe_cero}</td>
            <td class="text-right ${(row.importe_alto_obra ?? 0) > 0 ? 'quality-warn-cell' : ''}">${row.importe_alto_obra ?? 0}</td>
            <td class="text-right ${(row.importe_alto_otros ?? 0) > 0 ? 'quality-warn-cell' : ''}">${row.importe_alto_otros ?? 0}</td>
            <td class="text-right ${(row.exp_duplicado ?? 0) > 0 ? 'quality-warn-cell' : ''}">${row.exp_duplicado ?? 0}</td>
            <td class="text-right ${(row.sin_adjudicatario ?? 0) > 0 ? 'quality-warn-cell' : ''}">${row.sin_adjudicatario ?? 0}</td>
        </tr>
    `).join('');
}

function renderDashboard() {
    const summary = analysisData.summary;
    const cards = [
        { label: 'Total Contratos', value: summary.total_contracts, icon: 'file-text' },
        { label: 'Inversión Total', value: formatCurrency(summary.total_amount), icon: 'euro' },
        { label: 'Media por Contrato', value: formatCurrency(summary.avg_amount), icon: 'trending-up' },
        { label: 'Contratistas únicos', value: contractorsSummary.length.toLocaleString('es-ES'), icon: 'building-2' }
    ];

    const container = document.getElementById('summary-cards');
    container.innerHTML = cards.map(card => `
        <div class="card stat-card">
            <div style="display: flex; justify-content: space-between; align-items: start">
                <h4>${card.label}</h4>
                <div class="icon-circle">
                    <i data-lucide="${card.icon}" style="width: 16px; height: 16px"></i>
                </div>
            </div>
            <div class="value">${card.value}</div>
        </div>
    `).join('');
    lucide.createIcons();
}

function renderCharts() {
    const COLORS_MAIN  = ['#2563eb', '#7c3aed', '#d97706', '#059669', '#dc2626', '#0891b2'];
    const COLORS_ENTITY = ['#2563eb', '#7c3aed', '#d97706', '#059669', '#dc2626', '#0891b2', '#64748b'];

    const chartDefaults = {
        grid:  'rgba(0,0,0,0.06)',
        ticks: '#94a3b8',
        font:  { size: 11, family: "'Inter', sans-serif" },
    };

    // Utilidades compartidas entre gráficos
    const fmtK     = v => (v / 1000).toLocaleString('es-ES', { maximumFractionDigits: 0 }) + 'K€';
    const fmtM     = v => (v / 1_000_000).toLocaleString('es-ES', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + 'M€';
    const fmtSmart = v => v >= 1_000_000 ? fmtM(v) : fmtK(v);
    const fmtEur   = v => formatCurrency(v);

    // ── Gasto anual ──────────────────────────────────────────────────────
    const yearCtx = document.getElementById('yearChart').getContext('2d');
    const yearLabels = Object.keys(analysisData.by_year).sort();
    const yearValues = yearLabels.map(l => analysisData.by_year[l].sum);

    new Chart(yearCtx, {
        type: 'bar',
        plugins: [ChartDataLabels],
        data: {
            labels: yearLabels,
            datasets: [{
                label: 'Gasto (€)',
                data: yearValues,
                backgroundColor: 'rgba(37, 99, 235, 0.12)',
                borderColor: '#2563eb',
                borderWidth: 1.5,
                borderRadius: 4,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            layout: { padding: { top: 24 } },
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { label: ctx => ' ' + fmtM(ctx.parsed.y) } },
                datalabels: {
                    anchor: 'end', align: 'end',
                    color: chartDefaults.ticks,
                    font: { size: 11, family: "'Inter', sans-serif" },
                    formatter: fmtM,
                },
            },
            scales: {
                x: { grid: { display: false }, ticks: { color: chartDefaults.ticks, font: chartDefaults.font } },
                y: {
                    grid: { color: chartDefaults.grid },
                    ticks: { color: chartDefaults.ticks, font: chartDefaults.font,
                             callback: v => fmtM(v) }
                }
            }
        }
    });

    // ── Tipología de contratos ───────────────────────────────────────────
    const typeCtx = document.getElementById('typeChart').getContext('2d');
    const typeLabels = Object.keys(analysisData.by_type);
    const typeValues = typeLabels.map(l => analysisData.by_type[l].sum);
    const typeTotal = typeValues.reduce((a, b) => a + b, 0);

    typeChartInstance = new Chart(typeCtx, {
        type: 'doughnut',
        plugins: [ChartDataLabels],
        data: {
            labels: typeLabels,
            datasets: [{
                data: typeValues,
                backgroundColor: COLORS_MAIN,
                borderWidth: 0,
                spacing: 3,
                hoverOffset: 8,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: legendColor(),
                        font: { size: 13, family: "'Inter', sans-serif" },
                        padding: 20,
                        usePointStyle: true,
                        pointStyle: 'circle',
                        pointStyleWidth: 16,
                        boxWidth: 16,
                        boxHeight: 16,
                    }
                },
                tooltip: {
                    callbacks: {
                        label: ctx => {
                            const pct = Math.round(ctx.raw / typeTotal * 100);
                            return ` ${ctx.label}: ${fmtM(ctx.raw)} (${pct}%)`;
                        },
                    }
                },
                datalabels: {
                    color: '#fff',
                    font: { size: 13, weight: 'bold', family: "'Inter', sans-serif" },
                    // Ocultar etiqueta si el segmento es muy pequeño (< 3%) para evitar solapamiento
                    formatter: (value) => {
                        const pct = Math.round(value / typeTotal * 100);
                        return pct >= 3 ? pct + '%' : '';
                    },
                },
            }
        }
    });

    // ── Gasto por área (todas) ────────────────────────────────────────────
    const areaCtx = document.getElementById('areaChart').getContext('2d');
    const areaLabels = Object.keys(analysisData.by_area);
    const areaValues = areaLabels.map(l => analysisData.by_area[l].sum);

    new Chart(areaCtx, {
        type: 'bar',
        plugins: [ChartDataLabels],
        data: {
            labels: areaLabels,
            datasets: [{
                label: 'Euros (€)',
                data: areaValues,
                backgroundColor: 'rgba(37, 99, 235, 0.12)',
                borderColor: '#2563eb',
                borderWidth: 1.5,
                borderRadius: 3,
            }]
        },
        options: {
            responsive: true,
            indexAxis: 'y',  // barras horizontales para que quepan los nombres de área
            maintainAspectRatio: false,
            layout: { padding: { right: 56 } },
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { label: ctx => ' ' + fmtSmart(ctx.parsed.x) } },
                datalabels: {
                    anchor: 'end', align: 'end', clamp: false,
                    color: chartDefaults.ticks,
                    font: { size: 10, family: "'Inter', sans-serif" },
                    formatter: fmtSmart,
                },
            },
            scales: {
                x: {
                    grid: { color: chartDefaults.grid },
                    ticks: { color: chartDefaults.ticks, font: chartDefaults.font,
                             callback: v => fmtSmart(v) }
                },
                y: { grid: { display: false }, ticks: { color: chartDefaults.ticks, font: { size: 11, family: "'Inter', sans-serif" } } }
            }
        }
    });

    // ── Perfil de contratista ─────────────────────────────────────────────
    const entityCtx = document.getElementById('entityChart').getContext('2d');
    const entityRaw = analysisData.by_entity_type || {};
    const entityLabels = Object.keys(entityRaw);
    const entityValues = entityLabels.map(l => entityRaw[l].sum);
    const entityTotal = entityValues.reduce((a, b) => a + b, 0);

    entityChartInstance = new Chart(entityCtx, {
        type: 'doughnut',
        plugins: [ChartDataLabels],
        data: {
            labels: entityLabels,
            datasets: [{
                data: entityValues,
                backgroundColor: COLORS_ENTITY,
                borderWidth: 0,
                spacing: 3,
                hoverOffset: 8,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    onClick: (e, legendItem, legend) => {
                        legend.chart.toggleDataVisibility(legendItem.index);
                        legend.chart.update();
                    },
                    labels: {
                        color: legendColor(),
                        font: { size: 13, family: "'Inter', sans-serif" },
                        padding: 16,
                        usePointStyle: true,
                        pointStyle: 'circle',
                        pointStyleWidth: 16,
                        boxWidth: 16,
                        boxHeight: 16,
                        // Leyenda personalizada: muestra el importe total junto al nombre del tipo
                        generateLabels: (chart) => {
                            const data = chart.data;
                            const textColor = chart.options.plugins.legend.labels.color;
                            return data.labels.map((label, i) => ({
                                text: `${label}  ${fmtSmart(data.datasets[0].data[i])}`,
                                fillStyle: data.datasets[0].backgroundColor[i],
                                strokeStyle: 'transparent',
                                lineWidth: 0,
                                fontColor: textColor,
                                pointStyle: 'circle',
                                hidden: !chart.getDataVisibility(i),
                                index: i,
                            }));
                        },
                    }
                },
                tooltip: {
                    callbacks: {
                        label: ctx => {
                            const pct = Math.round(ctx.raw / entityTotal * 100);
                            return ` ${ctx.label}: ${fmtSmart(ctx.raw)} (${pct}%)`;
                        },
                    }
                },
                datalabels: {
                    color: '#fff',
                    font: { size: 13, weight: 'bold', family: "'Inter', sans-serif" },
                    formatter: (value) => {
                        const pct = Math.round(value / entityTotal * 100);
                        return pct >= 3 ? pct + '%' : '';
                    },
                },
            }
        }
    });
}

function renderTable() {
    const start = (currentPage - 1) * ITEMS_PER_PAGE;
    const end = start + ITEMS_PER_PAGE;
    const pageData = filteredContracts.slice(start, end);

    updateSortIndicators();

    const body = document.getElementById('contractsBody');
    body.innerHTML = pageData.map(c => `
        <tr>
            <td class="text-muted" style="width: 100px">${formatDate(c.fecha_adjudicacion)}</td>
            <td style="width: 25%"><strong>${c.adjudicatario_unificado || c.adjudicatario || '---'}</strong></td>
            <td class="hide-mobile" style="width: 90px">
                <div class="nif-cell">
                    <span class="nif-tag">${c.cif || '---'}</span>
                    ${c.tipo_entidad && c.tipo_entidad !== 'Desconocido' ? `<span class="entity-type type-${getEntityClass(c.tipo_entidad)}">${c.tipo_entidad}</span>` : ''}
                </div>
            </td>
            <td style="width: 40%">
                <div class="objeto-cell">
                    ${c.objeto}
                    <small class="tipo-tag ${c.tipo_contrato_limpio ? c.tipo_contrato_limpio.toLowerCase() : 'otros'}">${c.tipo_contrato_limpio || 'Otros'}</small>
                </div>
            </td>
            <td style="width: 150px"><span class="area-badge">${c.area || 'N/A'}</span></td>
            <td class="text-right" style="width: 110px"><strong>${formatCurrency(c.importe)}</strong></td>
        </tr>
    `).join('');

    renderPagination();
}

// Mapea el tipo de entidad a una clase CSS para el badge de color
function getEntityClass(type) {
    if (!type) return 'otros';
    const t = type.toLowerCase();
    if (t === 'sl') return 'sl';
    if (t === 'sa') return 'sa';
    if (t.includes('autónomo')) return 'autonomo';
    if (t.includes('pública')) return 'admin';
    if (t === 'asociación') return 'asociacion';
    if (t === 'cooperativa') return 'cooperativa';
    if (t === 'ute') return 'ute';
    if (t.includes('extranjera')) return 'extranjera';
    if (t.includes('religiosa')) return 'religiosa';
    if (t.includes('bienes')) return 'otros';
    if (t.includes('civil')) return 'otros';
    if (t.includes('otras')) return 'otros';
    return 'otros';
}


// ── 4. Filtros y búsqueda ────────────────────────────────────

function setupSorting() {
    document.querySelectorAll('th.sortable').forEach(th => {
        th.addEventListener('click', () => {
            const key = th.getAttribute('data-sort');
            if (sortKey === key) {
                // Mismo campo: invertir orden
                sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
            } else {
                // Campo nuevo: orden por defecto según tipo (numéricos/fecha → desc, texto → asc)
                sortKey = key;
                sortOrder = key === 'importe' || key === 'fecha_adjudicacion' ? 'desc' : 'asc';
            }
            sortData();
            renderTable();
        });
    });
}

function sortData() {
    filteredContracts.sort((a, b) => {
        let valA = a[sortKey];
        let valB = b[sortKey];

        if (sortKey === 'adjudicatario_unificado') {
            valA = (a.adjudicatario_unificado || a.adjudicatario || '').toLowerCase();
            valB = (b.adjudicatario_unificado || b.adjudicatario || '').toLowerCase();
        }

        // Nulos siempre al final
        if (valA == null) return 1;
        if (valB == null) return -1;

        if (typeof valA === 'string') {
            const res = valA.localeCompare(valB, 'es', { sensitivity: 'base' });
            return sortOrder === 'asc' ? res : -res;
        } else {
            return sortOrder === 'asc' ? valA - valB : valB - valA;
        }
    });
}

function updateSortIndicators() {
    document.querySelectorAll('th.sortable').forEach(th => {
        th.classList.remove('active');
        const icon = th.querySelector('i');
        if (icon) {
            icon.setAttribute('data-lucide', 'arrow-up-down');
        }

        if (th.getAttribute('data-sort') === sortKey) {
            th.classList.add('active');
            if (icon) {
                icon.setAttribute('data-lucide', sortOrder === 'asc' ? 'arrow-up' : 'arrow-down');
            }
        }
    });
    lucide.createIcons();
}

function renderPagination() {
    const totalPages = Math.ceil(filteredContracts.length / ITEMS_PER_PAGE);
    const container = document.getElementById('pagination');
    if (totalPages <= 1) {
        container.innerHTML = '';
        return;
    }
    let buttons = '';
    buttons += `<button class="page-btn" onclick="goToPage(1)" ${currentPage === 1 ? 'disabled' : ''}>&laquo;</button>`;
    buttons += `<button class="page-btn active">${currentPage} / ${totalPages}</button>`;
    buttons += `<button class="page-btn" onclick="goToPage(${currentPage + 1})" ${currentPage === totalPages ? 'disabled' : ''}>&raquo;</button>`;
    container.innerHTML = buttons;
}

// Expuesto en window para poder llamarlo desde el onclick inline del HTML
window.goToPage = (page) => {
    currentPage = page;
    renderTable();
    document.getElementById('contracts').scrollIntoView({ behavior: 'smooth' });
};

function populateFilters() {
    const anos = [...new Set(allContracts.map(c => c.year).filter(Boolean))].sort((a, b) => b - a);
    const areas = [...new Set(allContracts.map(c => c.area).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'es'));
    const tipos = [...new Set(allContracts.map(c => c.tipo_contrato_limpio).filter(Boolean))].sort();

    const anioSel = document.getElementById('filterAnio');
    anos.forEach(a => {
        const opt = document.createElement('option');
        opt.value = a;
        opt.textContent = a;
        anioSel.appendChild(opt);
    });

    const areaSel = document.getElementById('filterArea');
    areas.forEach(a => {
        const opt = document.createElement('option');
        opt.value = a;
        opt.textContent = a;
        areaSel.appendChild(opt);
    });

    const tipoSel = document.getElementById('filterTipo');
    tipos.forEach(t => {
        const opt = document.createElement('option');
        opt.value = t;
        opt.textContent = t;
        tipoSel.appendChild(opt);
    });

    const entidades = [...new Set(allContracts.map(c => c.tipo_entidad).filter(e => e && e !== 'Desconocido'))].sort((a, b) => a.localeCompare(b, 'es'));
    const entidadSel = document.getElementById('filterEntidad');
    entidades.forEach(e => {
        const opt = document.createElement('option');
        opt.value = e;
        opt.textContent = e;
        entidadSel.appendChild(opt);
    });

    // Filtro tipo de entidad en la pestaña Contratistas
    const entidadesCtors = [...new Set(contractorsSummary.map(c => c.tipo_entidad).filter(e => e && e !== 'Desconocido'))].sort((a, b) => a.localeCompare(b, 'es'));
    const entidadCtorSel = document.getElementById('contractorFilterEntidad');
    entidadesCtors.forEach(e => {
        const opt = document.createElement('option');
        opt.value = e;
        opt.textContent = e;
        entidadCtorSel.appendChild(opt);
    });
}

function applyFilters() {
    const { anio, trimestre, area, tipo, entidad, fechaDesde, fechaHasta, importeMin, importeMax, texto } = activeFilters;
    filteredContracts = allContracts.filter(c => {
        if (anio && String(c.year) !== anio) return false;
        if (trimestre && String(c.quarter) !== trimestre) return false;
        if (area && c.area !== area) return false;
        if (tipo && c.tipo_contrato_limpio !== tipo) return false;
        if (entidad && c.tipo_entidad !== entidad) return false;
        if (fechaDesde || fechaHasta) {
            // Comparación de strings YYYY-MM-DD (fecha_adjudicacion viene como "2023-01-04T00:00:00").
            // Los contratos sin fecha pasan el filtro: no se puede saber si están fuera de rango.
            // (Los 1502 contratos de 2022 no tienen fecha_adjudicacion en el dataset municipal.)
            const fechaStr = c.fecha_adjudicacion ? c.fecha_adjudicacion.substring(0, 10) : null;
            if (fechaStr && fechaStr !== 'None' && fechaStr !== 'NaT') {
                if (fechaDesde && fechaStr < fechaDesde) return false;
                if (fechaHasta && fechaStr > fechaHasta) return false;
            }
        }
        if (importeMin !== '' && c.importe < parseFloat(importeMin)) return false;
        if (importeMax !== '' && c.importe > parseFloat(importeMax)) return false;
        if (texto) {
            const q = texto.toLowerCase();
            return (
                (c.adjudicatario && c.adjudicatario.toLowerCase().includes(q)) ||
                (c.adjudicatario_unificado && c.adjudicatario_unificado.toLowerCase().includes(q)) ||
                (c.objeto && c.objeto.toLowerCase().includes(q)) ||
                (c.area && c.area.toLowerCase().includes(q)) ||
                (c.cif && String(c.cif).toLowerCase().includes(q))
            );
        }
        return true;
    });
    sortData();
    currentPage = 1;
    renderTable();
    updateResultsCount();
}

function updateResultsCount() {
    const total = filteredContracts.reduce((sum, c) => sum + (c.importe || 0), 0);
    const el = document.getElementById('resultsCount');
    el.innerHTML = `<strong>${filteredContracts.length.toLocaleString('es-ES')}</strong> contratos · <strong>${formatCurrency(total)}</strong>`;
}

function setupSearch() {
    document.getElementById('searchInput').addEventListener('input', e => {
        activeFilters.texto = e.target.value;
        applyFilters();
    });

    // Helper: marca el elemento con fondo amarillo si tiene valor activo
    // y gestiona has-value en el .filter-wrap padre (muestra/oculta el botón ×)
    function setFilterActive(el, active) {
        el.classList.toggle('filter-active', active);
        el.closest('.filter-wrap')?.classList.toggle('has-value', active);
    }
    // Para slicers: activo si cualquiera de sus inputs tiene valor
    function updateSlicerActive(groupId) {
        const group = document.getElementById(groupId);
        const hasValue = [...group.querySelectorAll('.slicer-input')].some(i => i.value !== '');
        group.classList.toggle('filter-active', hasValue);
    }

    document.getElementById('filterAnio').addEventListener('change', e => {
        activeFilters.anio = e.target.value;
        setFilterActive(e.target, e.target.value !== '');
        applyFilters();
    });

    document.getElementById('filterTrimestre').addEventListener('change', e => {
        activeFilters.trimestre = e.target.value;
        setFilterActive(e.target, e.target.value !== '');
        applyFilters();
    });

    document.getElementById('filterArea').addEventListener('change', e => {
        activeFilters.area = e.target.value;
        setFilterActive(e.target, e.target.value !== '');
        applyFilters();
    });

    document.getElementById('filterTipo').addEventListener('change', e => {
        activeFilters.tipo = e.target.value;
        setFilterActive(e.target, e.target.value !== '');
        applyFilters();
    });

    document.getElementById('filterEntidad').addEventListener('change', e => {
        activeFilters.entidad = e.target.value;
        setFilterActive(e.target, e.target.value !== '');
        applyFilters();
    });

    document.getElementById('filterFechaDesde').addEventListener('change', e => {
        activeFilters.fechaDesde = e.target.value;
        e.target.classList.toggle('has-value', !!e.target.value);
        updateSlicerActive('slicerFecha');
        applyFilters();
    });

    document.getElementById('filterFechaHasta').addEventListener('change', e => {
        activeFilters.fechaHasta = e.target.value;
        e.target.classList.toggle('has-value', !!e.target.value);
        updateSlicerActive('slicerFecha');
        applyFilters();
    });

    document.getElementById('filterImporteMin').addEventListener('input', e => {
        activeFilters.importeMin = e.target.value;
        updateSlicerActive('slicerImporte');
        applyFilters();
    });

    document.getElementById('filterImporteMax').addEventListener('input', e => {
        activeFilters.importeMax = e.target.value;
        updateSlicerActive('slicerImporte');
        applyFilters();
    });

    document.getElementById('clearFilters').addEventListener('click', () => {
        activeFilters = { anio: '', trimestre: '', area: '', tipo: '', entidad: '', fechaDesde: '', fechaHasta: '', importeMin: '', importeMax: '', texto: '' };
        ['filterAnio','filterTrimestre','filterArea','filterTipo','filterEntidad'].forEach(id => {
            const el = document.getElementById(id);
            el.value = '';
            el.classList.remove('filter-active');
            el.closest('.filter-wrap')?.classList.remove('has-value');
        });
        ['filterFechaDesde','filterFechaHasta','filterImporteMin','filterImporteMax'].forEach(id => {
            const el = document.getElementById(id);
            el.value = '';
            el.classList.remove('has-value');
        });
        ['slicerFecha','slicerImporte'].forEach(id => {
            document.getElementById(id).classList.remove('filter-active');
        });
        document.getElementById('searchInput').value = '';
        applyFilters();
    });

    // Botones × en selects de contratos: resetean el filtro disparando change
    document.querySelectorAll('#filtersPanel .filter-clear-btn[data-clear]').forEach(btn => {
        btn.addEventListener('click', () => {
            const target = document.getElementById(btn.dataset.clear);
            if (!target) return;
            target.value = '';
            target.dispatchEvent(new Event('change'));
        });
    });

    // Botones × en slicers de fecha e importe
    document.getElementById('clearSlicerFecha').addEventListener('click', () => {
        ['filterFechaDesde', 'filterFechaHasta'].forEach(id => {
            const el = document.getElementById(id);
            el.value = '';
            el.classList.remove('has-value');
        });
        activeFilters.fechaDesde = '';
        activeFilters.fechaHasta = '';
        updateSlicerActive('slicerFecha');
        applyFilters();
    });
    document.getElementById('clearSlicerImporte').addEventListener('click', () => {
        ['filterImporteMin', 'filterImporteMax'].forEach(id => { document.getElementById(id).value = ''; });
        activeFilters.importeMin = '';
        activeFilters.importeMax = '';
        updateSlicerActive('slicerImporte');
        applyFilters();
    });

    document.getElementById('downloadCsv').addEventListener('click', downloadCSV);

    // Búsqueda en la pestaña Contratistas (filtra por nombre, NIF y tipo de entidad)
    function applyContractorFilters() {
        const query = document.getElementById('contractorSearch').value.toLowerCase();
        const entidadEl = document.getElementById('contractorFilterEntidad');
        const entidad = entidadEl.value;
        // Resaltar filtro activo + mostrar botón ×
        entidadEl.classList.toggle('filter-active', entidad !== '');
        entidadEl.closest('.filter-wrap')?.classList.toggle('has-value', entidad !== '');
        filteredContractors = contractorsSummary.filter(c => {
            if (entidad && c.tipo_entidad !== entidad) return false;
            if (query) {
                return (
                    (c.nombre && c.nombre.toLowerCase().includes(query)) ||
                    (c.cif && c.cif.toLowerCase().includes(query))
                );
            }
            return true;
        });
        renderContractors();
    }

    document.getElementById('contractorSearch').addEventListener('input', applyContractorFilters);
    document.getElementById('contractorFilterEntidad').addEventListener('change', applyContractorFilters);

    // Botón × del filtro de tipo de entidad en contratistas
    document.querySelector('#contractorsView .filter-clear-btn[data-clear="contractorFilterEntidad"]')
        ?.addEventListener('click', () => {
            document.getElementById('contractorFilterEntidad').value = '';
            applyContractorFilters();
        });
}


// ── 5. Utilidades ────────────────────────────────────────────

function downloadCSV() {
    const headers = ['Fecha', 'Adjudicatario', 'NIF', 'Tipo entidad', 'Objeto', 'Área', 'Tipo contrato', 'Importe (€)', 'Expediente'];
    const rows = filteredContracts.map(c => [
        c.fecha_adjudicacion || '',
        c.adjudicatario_unificado || c.adjudicatario || '',
        c.cif || '',
        c.tipo_entidad || '',
        (c.objeto || '').replace(/"/g, '""'),
        c.area || '',
        c.tipo_contrato_limpio || '',
        c.importe != null ? c.importe.toLocaleString('es-ES', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '',
        c.expediente || '',
    ].map(v => `"${v}"`).join(','));

    const csv = [headers.join(','), ...rows].join('\n');
    // \uFEFF = BOM UTF-8: necesario para que Excel abra el CSV con tildes correctamente
    const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'contratos-rincon.csv';
    a.click();
    URL.revokeObjectURL(url);
}

function formatCurrency(val) {
    return new Intl.NumberFormat('es-ES', {
        style: 'currency', currency: 'EUR',
        useGrouping: true, minimumFractionDigits: 2, maximumFractionDigits: 2,
    }).format(val);
}

function formatDate(dateStr) {
    if (!dateStr || dateStr === 'None' || dateStr === 'NaT' || dateStr === 'null') return 'N/A';
    const date = new Date(dateStr);
    // Descartar fechas anteriores al año 2000 (artefactos de parseo del ETL)
    if (isNaN(date.getTime()) || date.getFullYear() < 2000) return 'N/A';
    return date.toLocaleDateString('es-ES');
}

document.addEventListener('DOMContentLoaded', init);
