import sys
from pathlib import Path

# beam_pipeline/ no es un paquete instalado; se agrega al sys.path para que
# los tests puedan hacer `import validation`, `import rules`, etc. igual que
# lo hace pipeline.py dentro del contenedor.
BEAM_PIPELINE_DIR = Path(__file__).resolve().parent.parent / "beam_pipeline"
sys.path.insert(0, str(BEAM_PIPELINE_DIR))
