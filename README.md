import pandas as pd
import json

# --- CONFIGURATION ---
CSV_PATH = 'pred_horaire_2025-11-24.csv'
OUTPUT_FILE = 'dashboard_final_light_sidebar.html'
TARGET_DATE = 'Lundi 24 Novembre 2025'

# --- 1. PRÉPARATION DES DONNÉES ---
try:
    df = pd.read_csv(CSV_PATH)
except FileNotFoundError:
    print(f"❌ Fichier {CSV_PATH} introuvable.")
    exit()

counters_data = []
# On groupe, on calcule le total, et on trie par volume décroissant
for name, group in df.groupby('name'):
    group = group.sort_values('hour')
    total_traffic = group['predicted_intensity'].sum()
    
    counters_data.append({
        "name": name,
        "lat": group['latitude'].iloc[0],
        "lon": group['longitude'].iloc[0],
        "total": int(total_traffic),
        "hourly": group['predicted_intensity'].tolist()
    })

# Tri décroissant (Top 10)
counters_data.sort(key=lambda x: x['total'], reverse=True)
counters_data = counters_data[:10]

# Couleurs dynamiques
vals = [c['total'] for c in counters_data]
series = pd.Series(vals)
q25, q50, q75 = series.quantile([0.25, 0.50, 0.75])

def get_color(val):
    if val < q25: return '#27ae60'      # Vert
    elif val < q50: return '#2980b9'    # Bleu
    elif val < q75: return '#f39c12'    # Orange
    else: return '#c0392b'              # Rouge

# Ajout des couleurs et formatage
for c in counters_data:
    c['color'] = get_color(c['total'])
    c['formatted_total'] = f"{c['total']:,}".replace(",", " ")

json_data = json.dumps(counters_data)

# --- 2. TEMPLATE HTML (Light + Sidebar + Chart) ---
html_content = f"""
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trafic Vélo - {TARGET_DATE}</title>
    
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.Default.css" />
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap" rel="stylesheet">

    <style>
        :root {{ 
            --sidebar-width: 320px; 
            --primary: #2c3e50; 
            --bg-light: #f4f6f8;
            --white: #ffffff;
            --border: #e0e0e0;
            --active-bg: #e3f2fd;
            --active-border: #3498db;
        }}

        body, html {{ margin: 0; padding: 0; height: 100%; font-family: 'Inter', sans-serif; background: var(--bg-light); color: var(--primary); overflow: hidden; }}
        
        /* LAYOUT */
        .app-container {{ display: flex; height: 100vh; width: 100vw; }}
        
        /* SIDEBAR GAUCHE */
        .sidebar {{
            width: var(--sidebar-width);
            min-width: var(--sidebar-width);
            background: var(--white);
            box-shadow: 2px 0 10px rgba(0,0,0,0.05);
            z-index: 1001;
            display: flex; flex-direction: column;
        }}

        .header {{ padding: 20px; border-bottom: 1px solid var(--border); background: #fff; }}
        .header h1 {{ margin: 0; font-size: 1.1rem; font-weight: 700; color: var(--primary); }}
        .header p {{ margin: 5px 0 0; font-size: 0.85rem; color: #7f8c8d; }}
        .date-badge {{ 
            display: inline-block; margin-top: 10px; 
            background: var(--active-bg); color: var(--active-border);
            padding: 4px 10px; border-radius: 12px; font-size: 0.8rem; font-weight: 600;
        }}

        .counter-list {{ flex: 1; overflow-y: auto; padding: 10px; }}

        .card {{
            background: var(--white); border: 1px solid var(--border); border-radius: 8px;
            padding: 12px; margin-bottom: 10px; cursor: pointer;
            transition: all 0.2s; display: flex; justify-content: space-between; align-items: center;
        }}
        .card:hover {{ background: #f8f9fa; border-color: #bdc3c7; transform: translateY(-2px); }}
        
        .card.active {{ 
            border-left: 4px solid var(--active-border); 
            background: var(--active-bg);
            border-color: var(--active-border);
        }}

        .card-info h3 {{ margin: 0; font-size: 0.9rem; color: #2c3e50; }}
        .rank {{ font-size: 0.75rem; color: #95a5a6; margin-right: 5px; }}
        .pill {{ font-size: 0.8rem; padding: 3px 8px; border-radius: 4px; color: white; font-weight: 600; }}

        /* MAP AREA */
        .map-wrapper {{ flex: 1; position: relative; height: 100%; }}
        #map {{ width: 100%; height: 100%; background: #e5e5e5; }}

        /* CHART PANEL (Flottant) */
        .chart-panel {{
            position: absolute; bottom: 20px; left: 20px; right: 20px; height: 260px;
            background: rgba(255, 255, 255, 0.95); border-radius: 12px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.15); padding: 15px;
            display: none; flex-direction: column; z-index: 1000;
            backdrop-filter: blur(5px); border: 1px solid rgba(0,0,0,0.05);
        }}
        .chart-header {{ display: flex; justify-content: space-between; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-bottom: 10px; }}
        .chart-title {{ font-weight: 600; color: var(--primary); }}
        .close-btn {{ cursor: pointer; color: #95a5a6; }}
        .close-btn:hover {{ color: var(--primary); }}
        
        /* CUSTOM MARKERS */
        .marker-cluster-small, .marker-cluster-medium, .marker-cluster-large {{
            background-color: rgba(52, 152, 219, 0.2) !important;
        }}
        .marker-cluster div {{
            background-color: #3498db !important; color: white !important;
        }}
    </style>
</head>
<body>

<div class="app-container">
    <div class="sidebar">
        <div class="header">
            <h1><i class="fa-solid fa-bicycle"></i> Prévisions Trafic</h1>
            <div class="date-badge">{TARGET_DATE}</div>
            <p>Top 10 des compteurs</p>
        </div>
        <div class="counter-list" id="counterList"></div>
    </div>

    <div class="map-wrapper">
        <div id="map"></div>
        
        <div class="chart-panel" id="chartPanel">
            <div class="chart-header">
                <span class="chart-title" id="chartTitle">...</span>
                <i class="fa-solid fa-xmark close-btn" onclick="closeChart()"></i>
            </div>
            <div style="flex:1; position: relative;">
                <canvas id="trafficChart"></canvas>
            </div>
        </div>
    </div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<script>
    const counters = {json_data};
    const markersMap = {{}};
    let chartInstance = null;

    // 1. CARTE (Style Light 'CartoDB Positron')
    const map = L.map('map', {{zoomControl: false}}).setView([43.6107, 3.8767], 13);
    L.control.zoom({{ position: 'topright' }}).addTo(map);
    
    L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
        attribution: '&copy; OpenStreetMap & CartoDB',
        maxZoom: 20
    }}).addTo(map);

    // Cluster
    const markersGroup = L.markerClusterGroup({{
        showCoverageOnHover: false, zoomToBoundsOnClick: true, spiderfyOnMaxZoom: true,
        removeOutsideVisibleBounds: true, spiderfyDistanceMultiplier: 2
    }});

    // --- FONCTION UNIFIEE D'INTERACTION ---
    function selectCounter(index, fromMap = false) {{
        const data = counters[index];

        // A. Highlight Liste
        document.querySelectorAll('.card').forEach(el => el.classList.remove('active'));
        const card = document.getElementById(`card-${{index}}`);
        if (card) {{
            card.classList.add('active');
            if (fromMap) card.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
        }}

        // B. Action Carte
        const marker = markersMap[index];
        if (marker && !fromMap) {{
            markersGroup.zoomToShowLayer(marker, function() {{
                marker.openPopup();
            }});
        }}

        // C. Afficher Graphique
        showChart(data);
    }}

    // --- GENERATION DU CONTENU ---
    const listContainer = document.getElementById('counterList');

    counters.forEach((c, index) => {{
        // 1. Marqueur
        const marker = L.circleMarker([c.lat, c.lon], {{
            radius: 9, fillColor: c.color, color: "#fff", weight: 3, opacity: 1, fillOpacity: 0.9
        }});
        
        marker.bindTooltip(`<b>${{c.name}}</b>`, {{ direction: 'top' }});
        
        marker.on('click', () => selectCounter(index, true));
        
        markersGroup.addLayer(marker);
        markersMap[index] = marker;

        // 2. Liste
        const card = document.createElement('div');
        card.className = 'card';
        card.id = `card-${{index}}`;
        card.innerHTML = `
            <div class="card-info">
                <span class="rank">#${{index+1}}</span>
                <h3>${{c.name}}</h3>
            </div>
            <div class="pill" style="background-color: ${{c.color}};">${{c.formatted_total}}</div>
        `;
        
        card.addEventListener('click', () => selectCounter(index, false));
        listContainer.appendChild(card);
    }});

    map.addLayer(markersGroup);

    // --- GRAPHIQUE CHART.JS ---
    function showChart(data) {{
        document.getElementById('chartPanel').style.display = 'flex';
        document.getElementById('chartTitle').innerText = data.name + " (Prévision Horaire)";

        const ctx = document.getElementById('trafficChart').getContext('2d');
        if(chartInstance) chartInstance.destroy();

        chartInstance = new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: Array.from({{length: 24}}, (_, i) => i + "h"),
                datasets: [{{
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
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{
                    y: {{ beginAtZero: true, grid: {{ color: '#f0f0f0' }} }},
                    x: {{ grid: {{ display: false }} }}
                }},
                interaction: {{ intersect: false, mode: 'index' }}
            }}
        }});
    }}

    function closeChart() {{
        document.getElementById('chartPanel').style.display = 'none';
    }}

</script>
</body>
</html>
"""

with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"✅ Dashboard Light Pro généré : {OUTPUT_FILE}")