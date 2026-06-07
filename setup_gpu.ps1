# One-shot GPU setup for DocumentRecognizer (Windows / PowerShell).
# PyTorch with CUDA MUST be installed before easyocr — otherwise pip resolves
# the CPU-only torch from PyPI as a transitive dependency.
#
# Usage (from project root, in your venv or base env):
#   powershell -ExecutionPolicy Bypass -File setup_gpu.ps1
$ErrorActionPreference = "Stop"

# Activate venv if present
if (Test-Path ".venv\Scripts\Activate.ps1") {
    . .venv\Scripts\Activate.ps1
} elseif (Test-Path "venv\Scripts\Activate.ps1") {
    . venv\Scripts\Activate.ps1
}

Write-Host "=== Step 1: PyTorch with CUDA 12.8 (cu128 — supports sm_120 / Blackwell) ==="
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu128

Write-Host "=== Step 2: project dependencies ==="
pip install -r requirements.txt

Write-Host "=== Verify PyTorch CUDA ==="
python -c @"
import torch
available = torch.cuda.is_available()
print('CUDA available:', available)
print('torch CUDA version:', torch.version.cuda)
if available:
    print('GPU:', torch.cuda.get_device_name(0))
else:
    print('ERROR: CUDA not available — check NVIDIA driver and PyTorch wheel')
    exit(1)
"@

Write-Host "=== Verify EasyOCR ==="
python -c @"
import easyocr
r = easyocr.Reader(['ru', 'en'], gpu=True, verbose=False)
print('EasyOCR GPU OK')
"@

Write-Host ""
Write-Host "Setup complete. Run the app with:"
Write-Host "  uvicorn app.main:app --host 0.0.0.0 --port 8007"
