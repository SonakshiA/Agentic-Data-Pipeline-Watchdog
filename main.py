import pandas as pd
import sqlite3
import os
from openai import AzureOpenAI
from langgraph.graph import StateGraph, END
from typing import TypedDict
from dotenv import load_dotenv

load_dotenv()

class PipelineState(TypedDict):
    df: pd.DataFrame
    issues: str
    cleaning_code: str
    step: str

client = AzureOpenAI(
    api_key= os.getenv("API_KEY"),
    api_version= os.getenv("API_VERSION"),
    azure_endpoint= os.getenv("AZURE_ENDPOINT")
)
DEPLOYMENT_NAME = "gpt-4o"

def load_data(_:PipelineState) -> PipelineState:
    df = pd.read_csv("titanic.csv")
    conn = sqlite3.connect("pipeline.db")
    df.to_sql("titanic_raw",conn,if_exists="replace",index=False)
    return {"df":df, "step":"loaded"}

def check_data_quality(state: PipelineState) -> PipelineState:
    df = state["df"]
    issues = {
        "missing_values" : df.isnull().sum().to_dict,
        "duplicates" : int(df.duplicated().sum()),
        "shape" : df.shape
    }

    return {**state, "issues": str(issues), "step":"checked"} #unpacking the dictionary into a new one and overwriting two keys

def agent_cleaning(state: PipelineState) -> PipelineState:
    issues = state["issues"]

    prompt = f"""
    You are a data quality agent in a pipeline.

    The dataset has the following issues:
    {issues}

    Write Python pandas code to clean the dataset (variable name: df)
    Rules:
    1. For all rows missing the age, fill it with the median age.
    2. Drop the column 'Cabin' if it exists.
    3. Drop duplicate rows, if any.

    Note:
    - The code will be executed using Python's `exec()` function.
    - Ensure the code is syntactically valid and does not include markdown, comments, or explanations.
    - Do not return anything other than executable Python code.

    """

    response = client.chat.completions.create(
        model = DEPLOYMENT_NAME,
        messages=[
            {"role":"system","content":"You are a data quality agent in a pipeline."},
            {"role":"user","content":prompt}
        ],
        temperature=0.42
    )

    cleaning_code = response.choices[0].message.content

    if cleaning_code.startswith("```"):
        cleaning_code = cleaning_code.strip("`").split("\n",1)[1]
        cleaning_code = cleaning_code.rsplit("```",1)[0]
    return {**state, "cleaning_code":cleaning_code, "step":"processed"}

def apply_cleaning(state: PipelineState) -> PipelineState:
    df = state["df"].copy()
    code = state["cleaning_code"]

    local_vars = {"df":df, "pd":pd}
    try:
        exec(code, {}, local_vars)
        df = local_vars["df"]
    except Exception as e:
        print("An exception occured: ", e)
    
    conn = sqlite3.connect("pipeline.db")
    df.to_sql("titanic_clean", conn, if_exists="replace", index=False)

    return {**state, "df": df, "step":"cleaned"}

# create the graph
workflow = StateGraph(PipelineState)

workflow.add_node("load",load_data)
workflow.add_node("check", check_data_quality)
workflow.add_node("process",agent_cleaning)
workflow.add_node("clean",apply_cleaning)

workflow.set_entry_point("load")

workflow.add_edge("load","check")
workflow.add_edge("check","process")
workflow.add_edge("process","clean")
workflow.add_edge("clean",END)

graph = workflow.compile()

# run pipeline
final_state = graph.invoke({}) # initial state is empty
print("Final state is: ", final_state["step"])