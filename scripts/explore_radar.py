import h5py
import pyart
import matplotlib.pyplot as plt

RADAR_FILE = "RMA3_0315_01_DBZH_20251230T151917Z.H5"

# 1. Leer con pyart
print("=== Información del Radar ===")
radar = pyart.aux_io.read_sinarame_h5(RADAR_FILE)

print(f"Campos disponibles: {list(radar.fields.keys())}")
print(f"Latitud radar: {radar.latitude['data'][0]}")
print(f"Longitud radar: {radar.longitude['data'][0]}")
print(f"Fecha/hora: {radar.time['units']}")
print(f"Número de sweeps (elevaciones): {radar.nsweeps}")
print(f"Elevaciones (grados): {radar.fixed_angle['data']}")

# 2. Extraer metadata del nombre del archivo
# RMA3_0315_01_DBZH_20251230T151917Z.H5
# RMA3 = Radar Meteorológico Argentino #3 (Formosa)
# 0315 = task/modo de escaneo
# 01 = volumen
# DBZH = variable (reflectividad horizontal)
# 20251230T151917Z = fecha y hora UTC

# 3. Visualizar los 3 PPIs que te piden (0.5°, 0.9°, 1.3°)
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

for i, (sweep, elev) in enumerate([(0, 0.5), (1, 0.9), (2, 1.3)]):
    display = pyart.graph.RadarDisplay(radar)
    ax = axes[i]
    plt.sca(ax)
    display.plot_ppi(
        "reflectivity", sweep=sweep, ax=ax, cmap="NWSRef", title=f"PPI {elev}°"
    )
    display.plot_range_rings([50, 100, 150, 200], ax=ax, col="gray", ls="--")

plt.tight_layout()
plt.savefig("radar_3_elevaciones.png", dpi=150)
print("\nImagen guardada: radar_3_elevaciones.png")

# 4. Mostrar info geográfica para generar tiles
print(f"\n=== Para generar tiles ===")
print(f"Centro del radar: ({radar.latitude['data'][0]}, {radar.longitude['data'][0]})")
print(f"Alcance máximo: {radar.range['data'][-1]/1000:.0f} km")
