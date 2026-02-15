import pyart
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS
import subprocess
from pathlib import Path
import matplotlib.colors as mcolors
import glob
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Configuración
OUTPUT_DIR = Path("output_radar")
GRID_RESOLUTION = 500  # metros por pixel
# Paletas de colores por variable
COLOR_PALETTES = {
    "DBZH": {
        "field": "reflectivity",
        "colors": [
                    # Zona Baja (Azules oscuros y grisáceos)
        (-15, (60, 66, 109, 255)),    # Gris azulado muy oscuro
        (-10, (61, 78, 123, 255)),   # Azul índigo oscuro
        (-5,  (61, 89, 136, 255)),   # Azul marino
        (0,   (60, 101, 150, 255)),   # Azul medio oscuro

        # Zona Transición (Azules claros a Cyan)
        (5,   (57, 113, 163, 255)),  # Azul rey/celeste oscuro
        (10,  (47, 137, 187, 255)),  # Celeste
        (15,  (38, 163, 209, 255)),   # Cyan / Turquesa

        # Zona Verde (Lluvia ligera a moderada)
        (20,  (77, 225, 51, 255)),   # Verde Lima brillante
        (25,  (58, 176, 39, 255)),   # Verde Hoja
        (30,  (36, 114, 23, 255)),     # Verde Bosque oscuro

        # Zona Convectiva (Amarillo a Rojo)
        (35,  (213, 217, 51, 255)),   # Amarillo puro
        (40,  (214, 151, 25, 255)),   # Naranja
        (45,  (193, 0, 23, 255)),     # Rojo brillante
        (50,  (194, 0, 95, 255)),     # Rojo oscuro / Granate

        # Zona Severa (Violetas a Blanco)
        (55,  (203, 0, 205, 255)),   # Púrpura oscuro
        (60,  (223, 246, 237, 255)),   # Magenta / Fucsia
        (65,  (167, 236, 207, 255)), # Blanco puro
        (70,  (135, 223, 190, 255)), # Cyan pálido / Hielo
        (75,  (135, 223, 190, 255)), # Verde menta muy pálido
        ],
    },
    "ZDR": {
        "field": "differential_reflectivity",
        "colors": [
            (-2, (0, 0, 150, 255)),
            (-1, (0, 100, 255, 255)),
            (0, (150, 150, 150, 255)),
            (1, (255, 255, 150, 255)),
            (2, (255, 200, 0, 255)),
            (3, (255, 150, 0, 255)),
            (4, (255, 50, 0, 255)),
            (6, (150, 0, 0, 255)),
        ],
    },
    "RHOHV": {
        "field": "cross_correlation_ratio",
        "colors": [
            (0.7, (150, 0, 150, 255)),
            (0.8, (100, 100, 255, 255)),
            (0.85, (0, 200, 255, 255)),
            (0.9, (0, 255, 150, 255)),
            (0.95, (150, 255, 0, 255)),
            (0.97, (255, 255, 0, 255)),
            (1.0, (255, 150, 0, 255)),
        ],
    },
    "KDP": {
        "field": "specific_differential_phase",
        "colors": [
            (-1, (100, 100, 100, 255)),
            (0, (0, 150, 255, 255)),
            (0.5, (0, 255, 200, 255)),
            (1, (0, 255, 0, 255)),
            (2, (255, 255, 0, 255)),
            (3, (255, 150, 0, 255)),
            (5, (255, 0, 0, 255)),
        ],
    },
    "VRAD": {
        "field": "velocity",
        "colors": [
            (-30, (0, 100, 255, 255)),
            (-20, (0, 180, 255, 255)),
            (-10, (100, 255, 255, 255)),
            (0, (200, 200, 200, 255)),
            (10, (255, 255, 100, 255)),
            (20, (255, 180, 0, 255)),
            (30, (255, 100, 0, 255)),
        ],
    },
}


def create_colormap(variable: str):
    """Crea colormap discreto (sin interpolación) para la variable especificada."""
    palette = COLOR_PALETTES.get(variable, COLOR_PALETTES["DBZH"])
    color_list = palette["colors"]

    values = [c[0] for c in color_list]
    colors = [np.array(c[1][:3]) / 255.0 for c in color_list]  # Solo RGB

    # BoundaryNorm: cada rango de valores tiene un color fijo, sin interpolación
    boundaries = values + [values[-1] + 5]
    norm = mcolors.BoundaryNorm(boundaries, len(colors))
    cmap = mcolors.ListedColormap(colors)

    return cmap, norm, min(values)


def read_radar(filepath: str):
    """Lee archivo H5 de radar con PyART."""
    print(f"Leyendo: {filepath}")
    radar = pyart.aux_io.read_sinarame_h5(filepath)
    print(f"  Variable: {list(radar.fields.keys())}")
    print(
        f"  Centro: ({radar.latitude['data'][0]:.4f}, {radar.longitude['data'][0]:.4f})"
    )

    # Agregar esta línea para ver el alcance máximo real:
    print(f"  Alcance máximo del radar: {radar.range['data'][-1]/1000:.0f} km")
    print(f"  Alcance limitado por código: 240 km")
    
    return radar


def radar_to_grid(radar, sweep: int = 0, resolution: int = 500):
    """Convierte datos polares de radar a grilla cartesiana lat/lon."""
    print(f"  Convirtiendo a grilla (sweep={sweep}, res={resolution}m)...")

    max_range = 240_000

    grid = pyart.map.grid_from_radars(
        (radar,),
        grid_shape=(
            1,
            int(2 * max_range / resolution),
            int(2 * max_range / resolution),
        ),
        grid_limits=(
            (0, 10000),
            (-max_range, max_range),
            (-max_range, max_range),
        ),
        fields=[list(radar.fields.keys())[0]],
        weighting_function="Barnes2",
        gridding_algo="map_gates_to_grid",
    )

    return grid


def grid_to_rgba_geotiff(grid, output_path: Path, field_name: str, variable: str):
    """Guarda la grilla como GeoTIFF RGBA de 8-bit con colores aplicados."""
    print(f"  Guardando GeoTIFF RGBA: {output_path}")

    data = grid.fields[field_name]["data"][0]
    data = np.ma.filled(data, np.nan).astype(np.float32)

    lon = grid.point_longitude["data"][0]
    lat = grid.point_latitude["data"][0]

    min_lon, max_lon = float(lon.min()), float(lon.max())
    min_lat, max_lat = float(lat.min()), float(lat.max())

    nrows, ncols = data.shape

    cmap, norm, min_val = create_colormap(variable)

    # Aplicar colormap discreto
    indices = norm(data)
    rgba = cmap(indices)
    rgba_uint8 = (rgba * 255).astype(np.uint8)

    # Alpha: opaco donde hay datos válidos >= min_val, transparente donde no
    rgba_uint8[:, :, 3] = 255
    mask = np.isnan(data) | (data < min_val)
    rgba_uint8[mask, 3] = 0

    transform = from_bounds(min_lon, min_lat, max_lon, max_lat, ncols, nrows)

    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=nrows,
        width=ncols,
        count=4,
        dtype=np.uint8,
        crs=CRS.from_epsg(4326),
        transform=transform,
        compress="lzw",
    ) as dst:
        for i in range(4):
            dst.write(np.flipud(rgba_uint8[:, :, i]), i + 1)

    print(
        f"    Bounds: ({min_lon:.2f}, {min_lat:.2f}) - ({max_lon:.2f}, {max_lat:.2f})"
    )


def geotiff_to_tiles(geotiff_path: Path, tiles_dir: Path, zoom_levels: str = "4-10"):
    """Genera tiles XYZ desde GeoTIFF usando gdal2tiles."""
    print(f"  Generando tiles en: {tiles_dir}")

    tiles_dir.mkdir(parents=True, exist_ok=True)

    num_processes = max(2, multiprocessing.cpu_count() // 2)

    cmd = [
        "gdal2tiles.py",
        "-p",
        "mercator",
        "-z",
        zoom_levels,
        "-w",
        "none",
        f"--processes={num_processes}",
        "--tiledriver=WEBP",
        str(geotiff_path),
        str(tiles_dir),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"    Error: {result.stderr}")
    else:
        print(f"    Tiles generados OK")


def process_radar_file(filepath: str, output_dir: Path, sweeps: list = [0, 1, 2]):
    """Procesa un archivo H5 de radar completo."""
    filename = Path(filepath).stem
    parts = filename.split("_")
    radar_id = parts[0]
    variable = parts[3]  # DBZH, ZDR, RHOHV, KDP, VRAD
    timestamp = parts[4]

    print(f"\n{'='*60}")
    print(f"Procesando: {radar_id} - {variable} - {timestamp}")
    print(f"{'='*60}")

    radar = read_radar(filepath)
    field_name = list(radar.fields.keys())[0]
    elevations = radar.fixed_angle["data"]
    print(f"  Elevaciones disponibles: {elevations[:5]}... (total: {len(elevations)})")

    # Procesar sweeps secuencialmente (más seguro para evitar problemas de concurrencia)
    for sweep in sweeps:
        elev = elevations[sweep]
        print(f"\n  --- Elevación {sweep}: {elev:.1f}° ---")

        # Estructura: output_radar/{radar_id}/{variable}/{timestamp}_elev{sweep}/
        sweep_dir = output_dir / radar_id / variable / f"{timestamp}_elev{sweep}"
        sweep_dir.mkdir(parents=True, exist_ok=True)

        grid = radar_to_grid(radar, sweep=sweep, resolution=GRID_RESOLUTION)

        geotiff_path = sweep_dir / f"{radar_id}_{variable}_{timestamp}_elev{sweep}.tif"
        grid_to_rgba_geotiff(grid, geotiff_path, field_name, variable)

        tiles_path = sweep_dir / "tiles"
        geotiff_to_tiles(geotiff_path, tiles_path)

    print(f"\n✅ Proceso completado. Output en: {output_dir}")


def process_all_radar_files(
    pattern: str = "RMA*.H5", output_dir: Path = OUTPUT_DIR, max_workers: int = None
):
    """Procesa todos los archivos H5 que coincidan con el patrón en paralelo.

    Filtra archivos según el volumen:
    - VRAD: solo volumen 02 (_02_VRAD_)
    - Otras variables (DBZH, ZDR, RHOHV, KDP): solo volumen 01 (_01_)
    """
    all_files = sorted(glob.glob(pattern))
    print(f"Archivos encontrados: {len(all_files)}")

    # Filtrar archivos según el volumen y variable
    files = []
    for filepath in all_files:
        filename = Path(filepath).stem
        parts = filename.split("_")

        if len(parts) >= 4:
            volume = parts[1]  # 0315
            subvolume = parts[2]  # 01 o 02
            variable = parts[3]  # DBZH, ZDR, RHOHV, KDP, VRAD

            if variable == "VRAD" and subvolume == "02":
                files.append(filepath)
                print(f"  ✓ {filename} (VRAD del volumen 02)")
            elif variable != "VRAD" and subvolume == "01":
                files.append(filepath)
                print(f"  ✓ {filename} ({variable} del volumen 01)")
            else:
                print(f"  ✗ {filename} (ignorado: {variable} en volumen {subvolume})")

    print(f"\nArchivos seleccionados para procesar: {len(files)}\n")

    if max_workers is None:
        max_workers = max(2, multiprocessing.cpu_count() // 2)

    print(f"Procesando con {max_workers} archivos en paralelo\n")

    # Procesar archivos en paralelo
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_radar_file, filepath, output_dir, [0, 1, 2]
            ): filepath
            for filepath in files
        }

        for future in as_completed(futures):
            filepath = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Error procesando {filepath}: {e}")


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    import os

    data_dir = "/data" if os.path.exists("/data") else "."
    pattern = f"{data_dir}/RMA*.H5"

    process_all_radar_files(pattern=pattern)
