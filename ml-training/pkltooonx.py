#!/usr/bin/env python3
import joblib
from onnxmltools import convert_lightgbm
from onnxmltools.convert.common.data_types import FloatTensorType

# === Config ===
pkl_model_path = "model.pkl"      # your existing joblib file
onnx_model_path = "model.onnx"    # output file
num_features = 11                 # change if your feature count differs

# === Load model ===
print(f"Loading model from {pkl_model_path} ...")
model = joblib.load(pkl_model_path)

# === Convert to ONNX ===
print("Converting to ONNX format with onnxmltools...")
initial_type = [('input', FloatTensorType([None, num_features]))]
onnx_model = convert_lightgbm(model, initial_types=initial_type)

# === Save ONNX file ===
with open(onnx_model_path, "wb") as f:
    f.write(onnx_model.SerializeToString())

print(f"[✓] ONNX model saved to {onnx_model_path}")
