import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import pandas as pd
    return (pd,)


@app.cell
def _(pd):
    agents_df = pd.read_excel("../data/agents_reworked.xlsx")

    agents_df
    return (agents_df,)


@app.cell
def _(agents_df, pd):
    from db import get_engine

    agents_df['start_date'] = pd.to_datetime(agents_df['start_date'], dayfirst=True)
    agents_df['end_date'] = pd.to_datetime(agents_df['end_date'], dayfirst=True)
    agents_df['type'] = agents_df['type'].fillna('')

    agents_df_to_db = agents_df[['fullname', 'id', 'type', 'start_date', 'end_date']]
    agents_df_to_db.columns = ['Name', 'NumberFromMinyst', 'Type', 'StartDate', 'EndDate']
    agents_df_to_db.to_sql(name="Agents", con=get_engine(), if_exists="append", index=False)
    return


if __name__ == "__main__":
    app.run()
