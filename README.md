# distributed real time fraud detection system Execution Instructions

This guide explains the step-by-step process to execute the machine learning pipeline for the `realtime-fraud-detection` project. Follow these instructions to generate data, train a model, convert it to ONNX format, and run the complete system using Docker Compose.

---

## 1. Generate Batch Data

Navigate to the appropriate directory and run the batch data generation script:

```bash
python generate_batch.py
```
*This step prepares the data required for model training.*

---

## 2. Train the ML Model- can be improved using GNN AND SHAP 

Go to the `ml_training` folder and execute the training script to produce a `.pkl` model file:

```bash
cd ml_training
python train_model.py
```
*After successful completion, a `.pkl` file will be generated in the `ml_training` folder.*

---

## 3. Convert the Model to ONNX Format

Convert the trained `.pkl` model to ONNX format:

```bash
python pkltoonnx.py
```
*This will create an ONNX model file.*

---

## 4. Place the ONNX Model in the Spark Job Folder

Move the generated ONNX file from the `ml_training` folder to the `spark_job` folder:

```bash
mv ml_training/model.onnx spark_job/
```
*The ONNX model is now ready to be used in the Spark job.*

---

## 5. Run Everything Using Docker Compose

In the root directory of the repository, start all services using Docker Compose:

```bash
docker compose up -d --build
```
*This command will launch the complete pipeline including data processing, model inference, kafka topic creation ,starting kafka ,spark ,logstash ,elastic search and kibana *

---

## Summary

- **Generate batch data**
- **Train ML model (.pkl)**
- **Convert to ONNX**
- **Place ONNX file in `spark_job/`**
- **Run `docker compose up` from root**

Follow these steps sequentially for a successful setup and execution. If you encounter any issues, please refer to the respective script documentation or seek assistance.
