# 🧼 Agentic Data Quality Pipeline
This project implements an automated data cleaning pipeline using an agentic approach powered by Azure OpenAI, LangGraph, and Pandas. It reads raw data, checks for quality issues, generates cleaning code via an LLM, and applies the cleaning—all orchestrated through a stateful graph.

## 🚀 Features
✅ Loads raw data from a CSV file into a SQLite database.
🔍 Checks for missing values, duplicates, and dataset shape.
🤖 Uses Azure OpenAI (gpt-4o) to generate cleaning code based on detected issues.
🧪 Executes the generated code safely using exec() in a sandboxed environment.
🗃️ Saves cleaned data back to the SQLite database.
🔄 Orchestrates the entire flow using LangGraph.

## 📦 Requirements
Python 3.8+
pandas
sqlite3
openai
langgraph
python-dotenv

## 🔐 Environment Setup
Create a .env file in the root directory with the following:
```
API_KEY=your_azure_openai_api_key
API_VERSION=2024-12-01-preview
AZURE_ENDPOINT=https://your-resource-name.openai.azure.com/
MODEL_DEPLOYMENT_NAME=gpt-4o
```

