import datetime
import papermill
import prefect
import nbconvert
import nbformat


@prefect.task
def execute_notebook(most_recent_day: datetime.date) -> str:
    output_path = "inspect-day-output.ipynb"
    papermill.execute_notebook(
        "inspect-day.ipynb",
        output_path,
        parameters=dict(most_recent_day=most_recent_day.strftime("%Y-%m-%d")),
    )
    return output_path


@prefect.task
def convert_notebook(notebook_path: str):
    pdf_path = notebook_path.replace(".ipynb", ".pdf")
    exporter = nbconvert.PDFExporter()
    notebook = nbformat.read(notebook_path, as_version=4)
    pdf_data, resources = exporter.from_notebook_node(notebook)

    with open(pdf_path, "wb") as f:
        f.write(pdf_data)
