import { useState, useEffect, useRef } from 'react';
import { useSpring, animated } from 'react-spring';
import { useGesture } from '@use-gesture/react';
import './App.css';
import farmMap from './assets/farm_background2.png';
import telemetryData from './final_mapped_data.json';

// --- Configuration Constants ---
const STATIC_ZOOM = 0.7;
const MAP_WIDTH = 2200;
const MAP_HEIGHT = 1200; 
const GRID_SIZE = 40;
const cols = Math.floor(MAP_WIDTH / GRID_SIZE);

const TOTAL_DAYS = 160;
const END_DATE = '2025-09-11';

const getStatusColor = (status) => {
  switch (status) {
    case 'critical': return 'rgba(211, 47, 47, 0.63)';
    case 'warning': return 'rgba(251, 193, 45, 0.48)';
    case 'optimal': return 'rgba(0, 174, 9, 0)'; 
    default: return 'transparent';
  }
};

function App() {
  const [selectedPlot, setSelectedPlot] = useState(null);
  const [gridLookup, setGridLookup] = useState({});
  const [sliderValue, setSliderValue] = useState(TOTAL_DAYS);
  const [farmAverages, setFarmAverages] = useState(null);
  const [isPlaying, setIsPlaying] = useState(false);
  
  const mapRef = useRef(null);
  const timerRef = useRef(null);

  // --- 1. Timelapse Logic ---
  useEffect(() => {
    if (isPlaying) {
      timerRef.current = setInterval(() => {
        setSliderValue((prev) => {
          if (prev >= TOTAL_DAYS) {
            setIsPlaying(false);
            return prev;
          }
          return prev + 1;
        });
      }, 80); // Speed: 150ms per day
    } else {
      clearInterval(timerRef.current);
    }
    return () => clearInterval(timerRef.current);
  }, [isPlaying]);

  // --- 2. Historical Data & Averaging Logic ---
  useEffect(() => {
    const baseDate = new Date(END_DATE);
    const targetDateObj = new Date(baseDate);
    targetDateObj.setDate(baseDate.getDate() - (TOTAL_DAYS - sliderValue));
    
    const targetDateString = targetDateObj.toISOString().split('T')[0];
    const lookup = {};
    const plotsArray = telemetryData.plots; 

    if (Array.isArray(plotsArray)) {
      const dailyData = plotsArray.filter(item => item.date === targetDateString);
      
      if (dailyData.length > 0) {
        const calculateAvg = (key) => 
          (dailyData.reduce((acc, curr) => acc + (curr[key] || 0), 0) / dailyData.length).toFixed(1);

        setFarmAverages({
          temp: calculateAvg('soil_temperature'),
          ambient: calculateAvg('ambient_temperature'),
          moisture: calculateAvg('soil_moisture'),
          ph: calculateAvg('soil_pH'),
          precip: calculateAvg('precipitation'),
          count: dailyData.length
        });
      }

      dailyData.forEach(item => {
        const cellId = item.plot_id; 
        let uiStatus = 'optimal';
        if (item.status === 'Highly Abnormal') uiStatus = 'critical';
        else if (item.status === 'Slightly Abnormal' || item.status === 'Warning') uiStatus = 'warning';

        lookup[cellId] = { ...item, status: uiStatus, cellId };
      });
    }
    
    setGridLookup(lookup);

    if (selectedPlot) {
      const updatedData = lookup[selectedPlot.cellId];
      if (updatedData) setSelectedPlot(updatedData);
    }
  }, [sliderValue]);

  // --- 3. Map Panning Logic ---
  const [{ x }, api] = useSpring(() => ({ 
    x: 0, 
    config: { mass: 1, tension: 200, friction: 30 } 
  }));

  useGesture(
    {
      onDrag: ({ offset: [dx] }) => {
        const scaledWidth = MAP_WIDTH * STATIC_ZOOM;
        const limitX = Math.max(0, (scaledWidth - window.innerWidth) / 2);
        const cappedX = Math.max(Math.min(dx, limitX), -limitX);
        api.start({ x: cappedX });
      },
    },
    { target: mapRef, drag: { from: () => [x.get(), 0], filterTaps: true } }
  );

  return (
    <div className="viewport">
      
      {/* --- Sidebar --- */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <h1>terral</h1>
          <p className="status-tag">Live Monitor</p>
        </div>
        
        <div className="sidebar-scroll-area">
          {selectedPlot ? (
            <div className="info-panel active-plot">
              <div className="info-divider" />
              <h3>Sector {selectedPlot.plot_id}</h3>
              <div className="info-row"><span className="label">Soil pH:</span><span className="value">{selectedPlot.soil_pH}</span></div>
              <div className="info-row"><span className="label">Moisture:</span><span className="value">{selectedPlot.soil_moisture}%</span></div>
              <div className="info-row"><span className="label">Soil Temp:</span><span className="value">{selectedPlot.soil_temperature}°C</span></div>
              <div className="info-row"><span className="label">Ambient:</span><span className="value">{selectedPlot.ambient_temperature}°C</span></div>
              <div className="info-row"><span className="label">Humidity:</span><span className="value">{selectedPlot.humidity} kg/m³</span></div>
              
              <div className="info-divider" />
              <div className="action-box">
                <p><strong>Action:</strong> {selectedPlot.action}</p>
              </div>
              <button className="close-btn" onClick={() => setSelectedPlot(null)}>Back to Overview</button>
            </div>
          ) : (
            <div className="info-panel">
              <div className="info-divider" />
              <h3>Farm Overview</h3>
              {farmAverages ? (
                <>
                  <div className="info-row"><span className="label">Active Plots:</span><span className="value">{farmAverages.count}</span></div>
                  <div className="info-row"><span className="label">Avg Soil Temp:</span><span className="value">{farmAverages.temp}°C</span></div>
                  <div className="info-row"><span className="label">Avg Ambient:</span><span className="value">{farmAverages.ambient}°C</span></div>
                  <div className="info-row"><span className="label">Avg Moisture:</span><span className="value">{farmAverages.moisture}%</span></div>
                  <div className="info-row"><span className="label">Avg pH:</span><span className="value">{farmAverages.ph}</span></div>
                  <div className="info-row"><span className="label">Avg Precip:</span><span className="value">{farmAverages.precip} mm</span></div>
                </>
              ) : (
                <p className="empty-state-text">Initializing telemetry...</p>
              )}
              <div className="info-divider" style={{ marginTop: '20px' }} />
              <p className="hint-text">Select a sector on the map for precision data.</p>
            </div>
          )}
        </div>
      </aside>

      {/* --- Map Viewport --- */}
      <animated.div 
        ref={mapRef} 
        className="map-container" 
        style={{ 
          transform: x.to(val => `translate(calc(-50% + ${val}px), -50%) scale(${STATIC_ZOOM})`),
          position: 'absolute', top: '50%', left: '50%', width: MAP_WIDTH, height: MAP_HEIGHT, touchAction: 'none'
        }}
      >
        <img src={farmMap} className="farm-image" draggable="false" alt="Farm Background" />
        
        <svg width={MAP_WIDTH} height={MAP_HEIGHT} style={{ position: 'absolute', top: 0, left: 0, pointerEvents: 'none' }}>
          <defs>
            <pattern id="grid-lines" width={GRID_SIZE} height={GRID_SIZE} patternUnits="userSpaceOnUse">
              <path d={`M ${GRID_SIZE} 0 L 0 0 0 ${GRID_SIZE}`} fill="none" stroke="rgba(255, 255, 255, 0.12)" strokeWidth="1" />
            </pattern>
          </defs>
          <rect width="100%" height="100%" fill="url(#grid-lines)" />

          {Object.entries(gridLookup).map(([id, data]) => {
            const cellId = parseInt(id);
            const rowIndex = Math.floor(cellId / cols);
            const colIndex = cellId % cols;
            const isSelected = selectedPlot?.cellId === cellId;

            return (
              <g key={cellId} style={{ pointerEvents: 'auto' }}>
                <rect
                  x={colIndex * GRID_SIZE} y={rowIndex * GRID_SIZE} width={GRID_SIZE} height={GRID_SIZE}
                  fill={getStatusColor(data.status)}
                  stroke={isSelected ? '#fff' : 'rgba(255, 255, 255, 0.3)'}
                  strokeWidth={isSelected ? 3 : 0.5}
                  style={{ cursor: 'crosshair', transition: 'all 0.2s', paintOrder: 'stroke' }}
                  onClick={() => setSelectedPlot(data)}
                />
              </g>
            );
          })}
        </svg>
      </animated.div>

      {/* --- Footer Controls --- */}
      <footer className="global-footer">
        <div className="slider-container">
          <div className="controls">
            <button className={`play-btn ${isPlaying ? 'playing' : ''}`} onClick={() => setIsPlaying(!isPlaying)}>
              {isPlaying ? 'PAUSE' : 'PLAY TIMELAPSE'}
            </button>
            <button className="reset-btn" onClick={() => { setIsPlaying(false); setSliderValue(0); }}>
              RESET
            </button>
          </div>

          <div className="slider-wrapper">
            <div className="slider-meta">
              <span className="label">Timeline Analysis</span>
              <span className="value">
                {(() => {
                  const d = new Date(END_DATE);
                  d.setDate(d.getDate() - (TOTAL_DAYS - sliderValue));
                  return d.toLocaleDateString('en-US', { 
                    month: 'long', day: '2-digit', year: 'numeric' 
                  });
                })()}
              </span>
            </div>
            <input 
              type="range" min="0" max={TOTAL_DAYS} value={sliderValue} 
              onChange={(e) => {
                setIsPlaying(false);
                setSliderValue(parseInt(e.target.value));
              }}
              className="white-slider-long"
            />
          </div>
        </div>
      </footer>
    </div>
  );
}

export default App;