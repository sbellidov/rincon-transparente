let allContracts = [];
let analysisData = {};
let contractorsSummary = [];
let filteredContracts = [];
let filteredContractors = [];
let currentPage = 1;
const ITEMS_PER_PAGE = 20;

let sortKey = 'fecha_adjudicacion';
let sortOrder = 'desc';

let activeFilters = { anio: '', trimestre: '', area: '', tipo: '', texto: '' };

async function init() {
    try {
        const [contractsRes, analysisRes, summaryRes] = await Promise.all([
            fetch('data/contracts.json?v=' + Date.now()),
            fetch('data/analysis.json?v=' + Date.now()),
            fetch('data/contractors_summary.json?v=' + Date.now())
        ]);

        allContracts = await contractsRes.json();
        analysisData = await analysisRes.json();
        contractorsSummary = await summaryRes.json();

        filteredContracts = [...allContracts];
        filteredContractors = [...contractorsSummary];

        sortData();
        renderDashboard();
        renderCharts();
        renderTable();
        renderContractors();
        populateFilters();
        setupSearch();
        setupSorting();
        setupTabs();
        updateResultsCount();
        lucide.createIcons();
    } catch (err) {
        console.error("Error loading data:", err);
    }
}

function setupTabs() {
    const tabs = document.querySelectorAll('.tab-btn');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const view = tab.getAttribute('data-view');

            // UI Update
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');

            document.querySelectorAll('.view-content').forEach(content => {
                content.classList.remove('active');
            });
            document.getElementById(`${view}View`).classList.add('active');
        });
    });
}

function renderContractors() {
    const container = document.getElementById('contractorsList');
    container.innerHTML = filteredContractors.map((c, idx) => `
        <div class="contractor-card" id="contractor-${c.cif}">
            <div class="contractor-header" onclick="toggleContractor('${c.cif}')">
                <div class="contractor-main-info">
                    <div class="contractor-rank">#${idx + 1}</div>
                    <div class="contractor-details">
                        <h3>${c.nombre || 'Desconocido'}</h3>
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

window.toggleContractor = (cif) => {
    const card = document.getElementById(`contractor-${cif}`);
    const isExpanded = card.classList.contains('expanded');

    // Close others
    // document.querySelectorAll('.contractor-card').forEach(c => c.classList.remove('expanded'));

    if (!isExpanded) {
        card.classList.add('expanded');
    } else {
        card.classList.remove('expanded');
    }
};

function renderDashboard() {
    const summary = analysisData.summary;
    const cards = [
        { label: 'Total Contratos', value: summary.total_contracts, icon: 'file-text' },
        { label: 'Inversión Total', value: formatCurrency(summary.total_amount), icon: 'euro' },
        { label: 'Media por Contrato', value: formatCurrency(summary.avg_amount), icon: 'trending-up' },
        { label: 'Contratista Principal', value: contractorsSummary[0].nombre.split(' ')[0] + '...', icon: 'award' }
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
                backgroundColor: 'rgba(56, 189, 248, 0.4)',
                borderColor: '#38bdf8',
                borderWidth: 1,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            indexAxis: 'y',
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#94a3b8', font: { size: 10 } } },
                y: { grid: { display: false }, ticks: { color: '#94a3b8', font: { size: 10 } } }
            }
        }
    });

    const yearCtx = document.getElementById('yearChart').getContext('2d');
    const yearLabels = Object.keys(analysisData.by_year).sort();
    const yearValues = yearLabels.map(l => analysisData.by_year[l].sum);

    new Chart(yearCtx, {
        type: 'line',
        data: {
            labels: yearLabels,
            datasets: [{
                label: 'Inversión',
                data: yearValues,
                borderColor: '#38bdf8',
                backgroundColor: 'rgba(56, 189, 248, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 4,
                pointBackgroundColor: '#38bdf8'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { display: false }, ticks: { color: '#94a3b8' } },
                y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#94a3b8' } }
            }
        }
    });

    const typeCtx = document.getElementById('typeChart').getContext('2d');
    const typeLabels = Object.keys(analysisData.by_type);
    const typeValues = typeLabels.map(l => analysisData.by_type[l].sum);

    new Chart(typeCtx, {
        type: 'doughnut',
        data: {
            labels: typeLabels,
            datasets: [{
                data: typeValues,
                backgroundColor: [
                    'rgba(56, 189, 248, 0.6)',
                    'rgba(139, 92, 246, 0.6)',
                    'rgba(245, 158, 11, 0.6)',
                    'rgba(239, 68, 68, 0.6)'
                ],
                borderColor: 'rgba(255, 255, 255, 0.1)',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: '#94a3b8', font: { size: 10 } }
                }
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
            <td style="width: 140px">
                <div class="cif-container" style="display: flex; align-items: center; gap: 0.5rem">
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

function getEntityClass(type) {
    if (!type) return 'otros';
    const t = type.toLowerCase();
    if (t.includes('sl')) return 'sl';
    if (t.includes('sa')) return 'sa';
    if (t.includes('autónomo')) return 'autonomo';
    if (t.includes('pública')) return 'admin';
    return 'otros';
}

function setupSorting() {
    document.querySelectorAll('th.sortable').forEach(th => {
        th.addEventListener('click', () => {
            const key = th.getAttribute('data-sort');
            if (sortKey === key) {
                sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
            } else {
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

window.goToPage = (page) => {
    currentPage = page;
    renderTable();
    document.getElementById('contracts').scrollIntoView({ behavior: 'smooth' });
};

function populateFilters() {
    const anos = [...new Set(allContracts.map(c => c.year).filter(Boolean))].sort();
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
}

function applyFilters() {
    const { anio, trimestre, area, tipo, texto } = activeFilters;
    filteredContracts = allContracts.filter(c => {
        if (anio && String(c.year) !== anio) return false;
        if (trimestre && String(c.quarter) !== trimestre) return false;
        if (area && c.area !== area) return false;
        if (tipo && c.tipo_contrato_limpio !== tipo) return false;
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

    document.getElementById('filterAnio').addEventListener('change', e => {
        activeFilters.anio = e.target.value;
        applyFilters();
    });

    document.getElementById('filterTrimestre').addEventListener('change', e => {
        activeFilters.trimestre = e.target.value;
        applyFilters();
    });

    document.getElementById('filterArea').addEventListener('change', e => {
        activeFilters.area = e.target.value;
        applyFilters();
    });

    document.getElementById('filterTipo').addEventListener('change', e => {
        activeFilters.tipo = e.target.value;
        applyFilters();
    });

    document.getElementById('clearFilters').addEventListener('click', () => {
        activeFilters = { anio: '', trimestre: '', area: '', tipo: '', texto: '' };
        document.getElementById('searchInput').value = '';
        document.getElementById('filterAnio').value = '';
        document.getElementById('filterTrimestre').value = '';
        document.getElementById('filterArea').value = '';
        document.getElementById('filterTipo').value = '';
        applyFilters();
    });

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

function formatCurrency(val) {
    return new Intl.NumberFormat('es-ES', { style: 'currency', currency: 'EUR' }).format(val);
}

function formatDate(dateStr) {
    if (!dateStr || dateStr === 'None' || dateStr === 'NaT' || dateStr === 'null') return 'N/A';
    const date = new Date(dateStr);
    if (isNaN(date.getTime()) || date.getFullYear() < 2000) return 'N/A';
    return date.toLocaleDateString('es-ES');
}

document.addEventListener('DOMContentLoaded', init);
