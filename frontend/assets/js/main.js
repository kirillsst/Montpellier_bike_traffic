document.addEventListener('DOMContentLoaded', () => {
    initDashboard(); // Charge la vue Carte + Données Prédictions
    loadStats();     // Charge la vue Statistiques (en arrière-plan)
});

// --- VARIABLES GLOBALES ---
let map, markersGroup;
const markersMap = {};
let chartInstance = null; // Pour le graphique "Prévision" (Map)
let countersData = [];

// =============================================================================
// 1. INITIALISATION & CHARGEMENT DONNÉES (DASHBOARD)
// =============================================================================

async function initDashboard() {
    initMap();

    try {
        // Chargement via l'API (Backend FastAPI)
        const response = await fetch('/api/dashboard-data');
        if (!response.ok) throw new Error("Erreur chargement API Dashboard");
        
        const jsonData = await response.json();
        countersData = jsonData.data;
        
        // Mise à jour de la date dans le header
        const badge = document.getElementById('dateBadge');
        if(badge) badge.innerText = jsonData.meta.date;

        // Génération des éléments visuels
        generateMarkers();
        generateList();

    } catch (error) {
        console.error("Erreur Dashboard:", error);
    }

    // Event Listener fermeture du panneau graphique
    const closeBtn = document.getElementById('closeChartBtn');
    if(closeBtn) closeBtn.addEventListener('click', closeChart);
}

function initMap() {
    // Carte centrée sur Montpellier
    map = L.map('map', {zoomControl: false}).setView([43.6107, 3.8767], 13);
    L.control.zoom({ position: 'topright' }).addTo(map);
    
    // Fond de carte "Light" (CartoDB Positron)
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap & CartoDB',
        maxZoom: 20
    }).addTo(map);

    // Groupe de Cluster pour gérer les superpositions
    markersGroup = L.markerClusterGroup({
        showCoverageOnHover: false, zoomToBoundsOnClick: true, spiderfyOnMaxZoom: true,
        removeOutsideVisibleBounds: true, spiderfyDistanceMultiplier: 2
    });
    map.addLayer(markersGroup);
}

function generateMarkers() {
    countersData.forEach((c, index) => {
        const marker = L.circleMarker([c.lat, c.lon], {
            radius: 9, fillColor: c.color, color: "#fff", weight: 3, opacity: 1, fillOpacity: 0.9
        });
        
        marker.bindTooltip(`<b>${c.name}</b>`, { direction: 'top' });
        
        // Clic sur marqueur -> Sélectionne le compteur
        marker.on('click', () => selectCounter(index, true));
        
        markersGroup.addLayer(marker);
        markersMap[index] = marker;
    });
}

function generateList() {
    const listContainer = document.getElementById('counterList');
    if(!listContainer) return;

    listContainer.innerHTML = ''; // Reset
    
    countersData.forEach((c, index) => {
        const card = document.createElement('div');
        card.className = 'card';
        card.id = `card-${index}`;
        card.innerHTML = `
            <div class="card-info">
                <span class="rank">#${index+1}</span>
                <h3>${c.name}</h3>
            </div>
            <div class="pill" style="background-color: ${c.color};">${c.formatted_total}</div>
        `;
        // Clic sur liste -> Sélectionne le compteur
        card.addEventListener('click', () => selectCounter(index, false));
        listContainer.appendChild(card);
    });
}

// =============================================================================
// 2. INTERACTION UTILISATEUR (SELECTION & GRAPHIQUE HORAIRE)
// =============================================================================

function selectCounter(index, fromMap = false) {
    const data = countersData[index];

    // A. Highlight Liste
    document.querySelectorAll('.card').forEach(el => el.classList.remove('active'));
    const card = document.getElementById(`card-${index}`);
    if (card) {
        card.classList.add('active');
        // Scroll auto seulement si clic sur la carte (pour éviter le saut si clic liste)
        if (fromMap) card.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }

    // B. Action Carte (Zoom si clic depuis la liste)
    const marker = markersMap[index];
    if (marker && !fromMap) {
        markersGroup.zoomToShowLayer(marker, () => marker.openPopup());
    }

    // C. Afficher Graphique
    updateChart(data);
}

function updateChart(data) {
    const panel = document.getElementById('chartPanel');
    const title = document.getElementById('chartTitle');
    if(panel) panel.style.display = 'flex';
    if(title) title.innerText = `${data.name} (Prévision Horaire)`;

    const ctx = document.getElementById('trafficChart').getContext('2d');
    
    // Détruire l'ancien graphique pour éviter les superpositions
    if (chartInstance) chartInstance.destroy();

    chartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: Array.from({length: 24}, (_, i) => i + "h"),
            datasets: [{
                label: 'Vélos',
                data: data.hourly,
                borderColor: '#3498db',
                backgroundColor: 'rgba(52, 152, 219, 0.1)',
                borderWidth: 3,
                fill: true,
                tension: 0.4,
                pointBackgroundColor: '#fff',
                pointBorderColor: '#3498db',
                pointRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                y: { beginAtZero: true, grid: { color: '#f0f0f0' } },
                x: { grid: { display: false } }
            },
            interaction: { intersect: false, mode: 'index' }
        }
    });
}

function closeChart() {
    const panel = document.getElementById('chartPanel');
    if(panel) panel.style.display = 'none';
}

// =============================================================================
// 3. NAVIGATION (ONGLETS CARTE / HISTORIQUE)
// =============================================================================

// Attaché à window pour être accessible depuis le HTML onclick=""
window.switchTab = function(tabName) {
    // Gestion des boutons actifs
    document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
    if(event && event.currentTarget) event.currentTarget.classList.add('active');

    // Visibilité Header Info
    const infoMap = document.getElementById('info-map');
    const infoStats = document.getElementById('info-stats');
    if(infoMap) infoMap.style.display = tabName === 'map' ? 'block' : 'none';
    if(infoStats) infoStats.style.display = tabName === 'stats' ? 'block' : 'none';
    
    // Visibilité Sidebar Content
    const list = document.getElementById('counterList');
    const kpi = document.getElementById('kpiList');
    if(list) list.style.display = tabName === 'map' ? 'block' : 'none';
    if(kpi) kpi.style.display = tabName === 'stats' ? 'block' : 'none';

    // Visibilité Vue Principale
    const viewMap = document.getElementById('view-map');
    const viewStats = document.getElementById('view-stats');
    if(viewMap) viewMap.style.display = tabName === 'map' ? 'flex' : 'none';
    if(viewStats) viewStats.style.display = tabName === 'stats' ? 'block' : 'none';
    
    // Hack Leaflet : Recalculer la taille de la carte quand on revient dessus
    if(tabName === 'map' && map) {
        setTimeout(() => { map.invalidateSize(); }, 100);
    }
};

// =============================================================================
// 4. GESTION DES STATISTIQUES (HISTORIQUE)
// =============================================================================

async function loadStats() {
    try {
        const response = await fetch('/api/stats-data');
        if (!response.ok) return; 
        
        const stats = await response.json();
        renderKPI(stats.kpi);
        renderCharts(stats);
        
    } catch (e) {
        console.warn("Stats API non disponible.");
    }
}

function renderKPI(kpi) {
    const container = document.getElementById('kpiList');
    if(!container) return;

    container.innerHTML = `
        <div class="kpi-card">
            <div class="kpi-value">${(kpi.total_bikes / 1000000).toFixed(1)} M</div>
            <div class="kpi-label">Vélos (10 capteurs)</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${kpi.avg_daily}</div>
            <div class="kpi-label">Moyenne / Jour (Cumul)</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-value">${kpi.total_days}</div>
            <div class="kpi-label">Jours analysés</div>
        </div>
        <div style="text-align: center; margin-top: 15px; font-size: 0.8rem; color: #95a5a6; font-style: italic;">
            * Données basées sur les 10 compteurs clés de notre étude.
        </div>
    `;
}

function renderCharts(stats) {
    // 1. Chart Mensuel
    const ctxMonthly = document.getElementById('monthlyChart');
    if(ctxMonthly) {
        new Chart(ctxMonthly, {
            type: 'line',
            data: {
                labels: stats.monthly.labels,
                datasets: [{
                    label: 'Trafic Mensuel',
                    data: stats.monthly.data,
                    borderColor: '#2c3e50',
                    backgroundColor: 'rgba(44, 62, 80, 0.1)',
                    fill: true,
                    tension: 0.3
                }]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }

    // 2. Chart Hebdomadaire
    const ctxWeekly = document.getElementById('weeklyChart');
    if(ctxWeekly) {
        new Chart(ctxWeekly, {
            type: 'bar',
            data: {
                labels: stats.weekly.labels,
                datasets: [{
                    label: 'Moyenne',
                    data: stats.weekly.data,
                    backgroundColor: ['#3498db', '#3498db', '#3498db', '#3498db', '#3498db', '#e74c3c', '#e74c3c']
                }]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }

    // 3. Chart Météo
    const ctxWeather = document.getElementById('weatherChart');
    if(ctxWeather) {
        new Chart(ctxWeather, {
            type: 'doughnut',
            data: {
                labels: stats.weather.labels,
                datasets: [{
                    data: stats.weather.data,
                    backgroundColor: ['#f1c40f', '#2980b9']
                }]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }
}