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
                    <table class="mini-contracts-table">
                        <thead>
                            <tr>
                                <th>Fecha</th>
                                <th>Objeto</th>
                                <th>Expediente</th>
                                <th class="text-right">Importe</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${c.contratos.map(contract => `
                                <tr>
                                    <td class="text-muted">${formatDate(contract.fecha_adjudicacion)}</td>
                                    <td>${contract.objeto}</td>
                                    <td class="text-muted">${contract.expediente || '---'}</td>
                                    <td class="text-right"><strong>${formatCurrency(contract.importe)}</strong></td>
                                </tr>
                            `).join('')}
                        </tbody>
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
    const isExpanded = card.classList.contains('expanded');
    if (!isExpanded) {
        card.classList.add('expanded');
    } else {
        card.classList.remove('expanded');
    }
};

function renderQuality() {
    const { total_checked, by_type = {}, by_year = [] } = auditData;

    const kpis = [
        { label: 'CIF inválido o faltante', value: (by_type['CIF Inválido'] || 0) + (by_type['CIF Faltante'] || 0), icon: 'fingerprint' },
        { label: 'Fecha inválida o faltante', value: by_type['Fecha Inválida/Faltante'] || 0, icon: 'calendar-x' },
        { label: 'Importe = 0 €', value: by_type['Importe Cero/Negativo'] || 0, icon: 'circle-off' },
        { label: 'Importe > 50.000 €', value: by_type['Importe Elevado'] || 0, icon: 'triangle-alert' },
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
            <td class="text-right ${row.sin_cif > 0 ? 'quality-warn-cell' : ''}">${row.sin_cif}</td>
            <td class="text-right ${row.sin_fecha > 0 ? 'quality-warn-cell' : ''}">${row.sin_fecha}</td>
            <td class="text-right ${row.importe_cero > 0 ? 'quality-warn-cell' : ''}">${row.importe_cero}</td>
            <td class="text-right ${row.importe_alto > 0 ? 'quality-warn-cell' : ''}">${row.importe_alto}</td>
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

    // ── Gasto anual ──────────────────────────────────────────────────────
    const yearCtx = document.getElementById('yearChart').getContext('2d');
    const yearLabels = Object.keys(analysisData.by_year).sort();
    const yearValues = yearLabels.map(l => analysisData.by_year[l].sum);

    new Chart(yearCtx, {
        type: 'bar',
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
            plugins: {
                legend: { display: false },
                datalabels: { display: false },
            },
            scales: {
                x: { grid: { display: false }, ticks: { color: chartDefaults.ticks, font: chartDefaults.font } },
                y: {
                    grid: { color: chartDefaults.grid },
                    ticks: { color: chartDefaults.ticks, font: chartDefaults.font,
                             callback: v => '€' + (v / 1000).toLocaleString('es-ES') + 'k' }
                }
            }
        }
    });

    // ── Tipología de contratos ───────────────────────────────────────────
    const typeCtx = document.getElementById('typeChart').getContext('2d');
    const typeLabels = Object.keys(analysisData.by_type);
    const typeValues = typeLabels.map(l => analysisData.by_type[l].sum);
    const typeTotal = typeValues.reduce((a, b) => a + b, 0);

    new Chart(typeCtx, {
        type: 'doughnut',
        plugins: [ChartDataLabels],
        data: {
            labels: typeLabels,
            datasets: [{
                data: typeValues,
                backgroundColor: COLORS_MAIN,
                borderColor: '#fff',
                borderWidth: 3,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: '#374151',
                        font: { size: 13, family: "'Inter', sans-serif" },
                        padding: 20,
                        usePointStyle: true,
                        pointStyleWidth: 12,
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
            plugins: {
                legend: { display: false },
                datalabels: { display: false },
            },
            scales: {
                x: {
                    grid: { color: chartDefaults.grid },
                    ticks: { color: chartDefaults.ticks, font: chartDefaults.font,
                             callback: v => '€' + (v / 1000).toLocaleString('es-ES') + 'k' }
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

    new Chart(entityCtx, {
        type: 'doughnut',
        plugins: [ChartDataLabels],
        data: {
            labels: entityLabels,
            datasets: [{
                data: entityValues,
                backgroundColor: COLORS_ENTITY,
                borderColor: '#fff',
                borderWidth: 3,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: '#374151',
                        font: { size: 13, family: "'Inter', sans-serif" },
                        padding: 16,
                        usePointStyle: true,
                        pointStyleWidth: 12,
                        // Leyenda personalizada: muestra el importe total junto al nombre del tipo
                        generateLabels: (chart) => {
                            const data = chart.data;
                            return data.labels.map((label, i) => ({
                                text: `${label}  ${formatCurrency(data.datasets[0].data[i])}`,
                                fillStyle: data.datasets[0].backgroundColor[i],
                                strokeStyle: data.datasets[0].backgroundColor[i],
                                pointStyle: 'circle',
                                index: i,
                            }));
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
                <div class="cif-cell">
                    <span class="cif-tag">${c.cif || '---'}</span>
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
    if (t.includes('sl')) return 'sl';
    if (t.includes('sa')) return 'sa';
    if (t.includes('autónomo')) return 'autonomo';
    if (t.includes('pública')) return 'admin';
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
            // Comparación de strings YYYY-MM-DD para evitar desfase por zona horaria
            // (fecha_adjudicacion viene como "2023-01-04T00:00:00", hora local)
            const fechaStr = c.fecha_adjudicacion ? c.fecha_adjudicacion.substring(0, 10) : null;
            if (!fechaStr || fechaStr === 'None' || fechaStr === 'NaT') return false;
            if (fechaDesde && fechaStr < fechaDesde) return false;
            if (fechaHasta && fechaStr > fechaHasta) return false;
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
    function setFilterActive(el, active) {
        el.classList.toggle('filter-active', active);
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
        updateSlicerActive('slicerFecha');
        applyFilters();
    });

    document.getElementById('filterFechaHasta').addEventListener('change', e => {
        activeFilters.fechaHasta = e.target.value;
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
        });
        ['filterFechaDesde','filterFechaHasta','filterImporteMin','filterImporteMax'].forEach(id => {
            document.getElementById(id).value = '';
        });
        ['slicerFecha','slicerImporte'].forEach(id => {
            document.getElementById(id).classList.remove('filter-active');
        });
        document.getElementById('searchInput').value = '';
        applyFilters();
    });

    document.getElementById('downloadCsv').addEventListener('click', downloadCSV);

    // Búsqueda en la pestaña Contratistas (filtra por nombre o CIF)
    const contractorSearch = document.getElementById('contractorSearch');
    contractorSearch.addEventListener('input', (e) => {
        const query = e.target.value.toLowerCase();
        filteredContractors = contractorsSummary.filter(c =>
            (c.nombre && c.nombre.toLowerCase().includes(query)) ||
            (c.cif && c.cif.toLowerCase().includes(query))
        );
        renderContractors();
    });
}


// ── 5. Utilidades ────────────────────────────────────────────

function downloadCSV() {
    const headers = ['Fecha', 'Adjudicatario', 'CIF', 'Tipo entidad', 'Objeto', 'Área', 'Tipo contrato', 'Importe (€)', 'Expediente'];
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
