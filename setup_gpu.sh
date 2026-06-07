#!/usr/bin/env bash
# One-shot GPU setup for DocumentRecognizer.
# PyTorch with CUDA MUST be installed before easyocr — otherwise pip resolves
# the CPU-only torch from PyPI as a transitive dependency.
set -e

echo "=== Step 1: PyTorch with CUDA 12.8 (cu128 — supports sm_120 / Blackwell) ==="
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu128

echo "=== Step 2: project dependencies ==="
pip install -r requirements.txt

echo "=== Verify PyTorch CUDA ==="
python -c "
import torch
available = torch.cuda.is_available()
print('CUDA available:', available)
print('torch CUDA version:', torch.version.cuda)
if available:
    print('GPU:', torch.cuda.get_device_name(0))
else:
    print('ERROR: CUDA not available — check NVIDIA driver and PyTorch wheel')
    exit(1)
"

echo "=== Verify EasyOCR ==="
python -c "
import easyocr
r = easyocr.Reader(['ru', 'en'], gpu=True, verbose=False)
print('EasyOCR GPU OK')
"

echo ""
echo "Setup complete. Run the app with:"
echo "  uvicorn app.main:app --host 0.0.0.0 --port 8007"
