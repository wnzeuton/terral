const express = require('express');
const cors = require('cors');
const app = express();
app.use(cors());

app.get('/api/map-layer', (req, res) => {
  const geoData = {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        properties: { id: "zone_1", type: "Field Zone", moisture: 22, name: "North Field" },
        geometry: { type: "Polygon", coordinates: [[[10, 10], [400, 10], [400, 300], [10, 300], [10, 10]]] }
      },
      {
        type: "Feature",
        properties: { id: "hub_1", type: "Sensor Hub", status: "Low Battery", name: "Hub A1" },
        geometry: { type: "Point", coordinates: [200, 150] }
      },
      {
        type: "Feature",
        properties: { id: "valve_1", type: "Master Valve", status: "High Pressure", name: "Main Valve" },
        geometry: { type: "Point", coordinates: [50, 50] }
      }
    ]
  };
  res.json(geoData);
});

app.listen(5000, () => console.log("Agri-Server running on port 5000"));