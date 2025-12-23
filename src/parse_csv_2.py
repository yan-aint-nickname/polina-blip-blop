import marimo

__generated_with = "0.18.4"
app = marimo.App()


@app.cell
def _():
    import pandas as pd
    from sqlalchemy import text
    return pd, text


@app.cell
def _(pd):
    part1 = pd.read_csv(
        "../data/foreign_agents_part1_extended.csv", index_col="id", sep=";"
    )
    part2 = pd.read_csv(
        "../data/foreign_agents_part2_extended.csv", index_col="id", sep=";"
    )

    df = pd.concat([part1, part2])
    df.dropna(subset=['name'], inplace=True)

    df.to_csv("../data/foreign_agents_combined_extended.csv", sep=";")
    # df
    return (df,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Заполняем бд
    """)
    return


@app.cell
def _():
    from db import get_engine

    engine = get_engine()
    return (engine,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Добавляем Occupations агентов
    """)
    return


@app.cell
def _(df, engine, pd, text):
    # extract occupations
    import itertools
    df_combined = pd.read_csv("../data/foreign_agents_combined_extended.csv", sep=";", index_col="id")

    df["area"] = df["area"].str.lower()
    df["occupation"] = df["occupation"].str.lower()

    areas = df[["area", "occupation"]].dropna(subset=['area', 'occupation'], how='all')


    # Вставляем в бд
    insert_query_occupations = text("""
        INSERT INTO "Occupations" ("Title", "Area")
        VALUES (:title, :area)
        ON CONFLICT ("Title", "Area") DO NOTHING;
    """)


    with engine.connect() as conn_occup:
        for i, row in areas.iterrows():
            a = row["area"].split(',') if isinstance(row["area"], str) else []
            o = row["occupation"].split(',')
            areas_occupations_to_db = list(itertools.zip_longest(a, o))
            for x in areas_occupations_to_db:
                title = x[1]
                area = x[0]
                if not title and not area:
                    continue
                if not title:
                    title = area
                try:
                    conn_occup.execute(insert_query_occupations, {"title": title.strip(), "area": area.strip() if area else None})
                except Exception as e:
                    print(e)
        conn_occup.commit()
    return df_combined, itertools


@app.cell
def _(mo):
    mo.md(r"""
    ### Добавляем Законы(Кодексы)
    """)
    return


@app.cell
def _(engine, text):
    from datetime import datetime
    # Добавляем кодексы 
    laws_data = [
        {
            "type": "УК РФ",
            "title": "Уголовный кодекс Российской Федерации",
            "start_date": datetime.strptime("13.06.1996", "%d.%m.%Y").date()
        },
        {
            "type": "КоАП РФ",
            "title": "Кодекс Российской Федерации об административных правонарушениях",
            "start_date": datetime.strptime("30.12.2001", "%d.%m.%Y").date()
        }
    ]

    with engine.connect() as conn_insert_laws:
        conn_insert_laws.execute(text(f'INSERT INTO "Laws" ("Type", "Title", "StartDate") VALUES (:type, :title, :start_date)'), laws_data)
        conn_insert_laws.commit()
    return


@app.cell
def _(engine, pd):
    # Добавляем статьи законов
    df_cc = pd.read_csv("../data/criminal_code.csv", sep=";")
    df_ac = pd.read_csv("../data/administrative_code.csv", sep=",")
    df_all = pd.concat([df_cc, df_ac], ignore_index=True)

    articles_to_db = df_all[["article_number", "name", "law_type"]].copy()
    articles_to_db.columns = ["Number", "Name", "LawType"]

    articles_to_db.to_sql(name="Articles", con=engine, if_exists="append", index=False)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Добавляем регионы Regions
    """)
    return


@app.cell
def _(pd):
    regions = pd.read_csv("../data/regions.csv")
    # regions
    return (regions,)


@app.cell
def _(engine, regions):
    print("Кол-во регионов", len(regions))

    regions_to_db = regions[["subjectCode", "nameRU"]].copy()
    regions_to_db.columns = ["Id", "Name"]

    regions_to_db.to_sql(name="Regions", con=engine, if_exists="append", index=False)
    return


@app.cell
def _(engine, pd):
    # Проверяем кол-во регионов
    regions_from_db = pd.read_sql('select * from "Regions"', con=engine)
    print(len(regions_from_db))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Обрабатываем судебные дела
    """)
    return


@app.cell
def _(engine, pd):
    cases_parsed = pd.read_csv("../data/agents_casesss.csv", sep=";")


    cases_parsed[["CaseNumber"]].to_sql(name="Cases", con=engine, if_exists="append", index=False)
    return (cases_parsed,)


@app.cell
def _(mo):
    mo.md(r"""
    #### Обрабатываем судей Judges
    """)
    return


@app.cell
def _(cases_parsed, engine, pd):
    # Сохраняем судей из csv в бд
    full_names = pd.DataFrame({"Name": cases_parsed["Judge"].unique()})
    print("Кол-во судей", len(full_names))

    full_names.to_sql(name="Judges", con=engine, if_exists="append", index=False)
    return


@app.cell
def _(engine, pd):
    # Проверяем кол-во судей в бд
    judges_from_db = pd.read_sql('select * from "Judges"', engine)
    print(len(judges_from_db.index))
    return (judges_from_db,)


@app.cell
def _(mo):
    mo.md(r"""
    #### Обрабатываем суды(здания) Courts
    """)
    return


@app.cell
def _(cases_parsed, engine):
    unique_courts = cases_parsed["Court"].unique()

    print("Кол-во судов", len(unique_courts))

    courtes_parsed_to_db = cases_parsed[["Court", "CourtRegion"]].copy().drop_duplicates(subset=["Court", "CourtRegion"])
    courtes_parsed_to_db.columns = ["Name", "RegionId"]

    courtes_parsed_to_db.to_sql(
        name="Courts", con=engine, if_exists="append", index=False
    )
    return


@app.cell
def _(engine, pd):
    # Проверяем кол-во судов в бд
    courtes_from_db = pd.read_sql('select * from "Courts"', engine)
    print(len(courtes_from_db))
    return


@app.cell
def _(mo):
    mo.md(r"""
    #### Обрабатываем Cases
    """)
    return


@app.cell
def _(cases_parsed, engine, pd):
    # Сохраняем дела из csv в бд
    cases = pd.DataFrame({"CaseNumber": cases_parsed["CaseNumber"].unique()})
    print("Кол-во дел", len(cases))

    cases.to_sql(name="Cases", con=engine, if_exists="append", index=False)
    return


@app.cell
def _(engine, pd):
    # Проверяем кол-во дел в бд
    cases_from_db = pd.read_sql('select * from "Cases"', engine)
    print(len(cases_from_db.index))
    return (cases_from_db,)


@app.cell
def _(mo):
    mo.md(r"""
    #### Обрабатываем Judges Cases
    """)
    return


@app.cell
def _(cases_from_db, cases_parsed, engine, judges_from_db, pd):
    from sqlalchemy.dialects.postgresql import UUID

    df_cases_merged = pd.merge(
        cases_parsed, 
        cases_from_db[['Id', 'CaseNumber']], 
        on="CaseNumber", 
        how="inner"
    )

    df_cases_judgest_merged = pd.merge(
        cases_parsed[['CaseNumber', 'Judge']], 
        judges_from_db, 
        left_on="Judge", 
        right_on="Name", 
        how="inner"
    )

    df_cases_judgest_merged = df_cases_judgest_merged[['CaseNumber', 'Judge', 'Id']]
    df_cases_judgest_merged.rename(columns={"Id": "JudgeId"}, inplace=True)

    df_cases_merged.rename(columns={"Id": "CaseId"}, inplace=True)
    df_cases_merged = df_cases_merged[['CaseId', 'CaseNumber']]


    judges_cases_final = pd.merge(
        df_cases_judgest_merged, 
        df_cases_merged, 
        on="CaseNumber", 
        how="inner"
    )

    judges_cases_to_db = judges_cases_final[['JudgeId', 'CaseId']].drop_duplicates()
    judges_cases_to_db.to_sql("JudgesCases", con=engine, if_exists="append", index=False, dtype={
            'JudgeId': UUID,
            'CaseId': UUID
        })
    judges_cases_to_db
    return (UUID,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Добавляем CasesArticles
    """)
    return


@app.cell
def _(UUID, cases_parsed, engine, pd):
    import re
    import logging

    df_cases_from_db = pd.read_sql('SELECT * FROM "Cases"', engine)

    df_cases_parsed_copy = cases_parsed.copy()

    df_with_uuids = df_cases_parsed_copy.merge(
        df_cases_from_db[['Id', 'CaseNumber']], 
        left_on='CaseNumber', 
        right_on='CaseNumber'
    )

    # df_with_uuids

    df_articles_from_db = pd.read_sql('SELECT "Number", "LawType" FROM "Articles"', engine)

    def parse_article_robust(text):
        if not isinstance(text, str):
            return None, None

        num_match = re.search(r"ст\.\s*([\d\.]+)", text, re.IGNORECASE)
        number = num_match.group(1) if num_match else None


        law_type = None
        if "УК" in text:
            law_type = "УК РФ"
        elif "КоАП" in text:
            law_type = "КоАП РФ"

        return number, law_type


    df_proc = cases_parsed[['CaseNumber', 'Article']].copy()
    df_proc[['ExtractedNumber', 'ExtractedLawType']] = df_proc['Article'].apply(
        lambda x: pd.Series(parse_article_robust(x))
    )

    articles_laws_cases_merged = pd.merge(
        df_proc,
        df_cases_from_db[['Id', 'CaseNumber']],
        on="CaseNumber",
        how="inner"
    ).rename(columns={'Id': 'CaseId'})

    final_links = pd.merge(
        articles_laws_cases_merged,
        df_articles_from_db,
        left_on=['ExtractedNumber', 'ExtractedLawType'],
        right_on=['Number', 'LawType'],
        how='inner'
    )

    cases_articles_to_insert = final_links[['Number', 'LawType', 'CaseId']].copy()
    cases_articles_to_insert.columns = ['ArticleNumber', 'ArticleLawType', 'CaseId']
    cases_articles_to_insert = cases_articles_to_insert.drop_duplicates()

    cases_articles_to_insert.to_sql("CasesArticles", con=engine, if_exists="append", index=False, dtype={'CaseId': UUID})
    cases_articles_to_insert
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Добавляем AgentsCases
    """)
    return


@app.cell
def _(UUID, cases_parsed, engine, pd):
    agents_db = pd.read_sql('SELECT "Id" as "AgentId", "Name" FROM "Agents"', engine)
    cases_db = pd.read_sql('SELECT "Id" as "CaseId", "CaseNumber" FROM "Cases"', engine)

    mapped_df = cases_parsed.merge(agents_db, left_on='FIO', right_on='Name', how='left')

    mapped_df = mapped_df.merge(cases_db, on='CaseNumber', how='left')

    agents_cases_to_insert = mapped_df[['AgentId', 'CaseId']].dropna()

    agents_cases_to_insert.to_sql('AgentsCases', engine, if_exists='append', index=False, dtype={'CaseId': UUID, 'AgentId': UUID})
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Добавляем CourtCases
    """)
    return


@app.cell
def _(UUID, cases_from_db, cases_parsed, engine, pd):
    courts_from_db = pd.read_sql('SELECT "Id" as "CourtId", "Name", "RegionId" FROM "Courts"', engine)

    cc_payload = cases_parsed.copy()

    cc_payload["CourtRegion"] = cc_payload["CourtRegion"].astype(str).str.zfill(2)

    cc_payload = cc_payload.merge(
        courts_from_db, 
        left_on=["Court", "CourtRegion"], 
        right_on=["Name", "RegionId"], 
        how="inner"
    )

    cc_payload = cc_payload.merge(cases_from_db, on="CaseNumber", how="inner")

    # cc_payload.columns

    cc_final = cc_payload[[
        "CourtId", 
        "Id", 
        "Court_instance", 
        "StartDate", 
        "DecisionDate", 
        "Desicion"
    ]].rename(columns={
        "Id": "CaseId",
        "Court_instance": "InstanceLevel",
        "StartDate": "EntryDate",
        "Desicion": "Decision"
    })

    cc_final["EntryDate"] = pd.to_datetime(cc_final["EntryDate"], dayfirst=True, errors='coerce')
    cc_final["DecisionDate"] = pd.to_datetime(cc_final["DecisionDate"], dayfirst=True, errors='coerce')

    cc_final = cc_final.dropna(subset=["EntryDate"]).drop_duplicates(subset=["CourtId", "CaseId"])

    cc_final = cc_final.dropna(subset=["EntryDate", "InstanceLevel"])
    cc_final.to_sql("CourtsCases", engine, if_exists="append", index=False, dtype={"CourtId": UUID, "CaseId": UUID})
    cc_final
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### AgentsOccupations
    """)
    return


@app.cell
def _(UUID, df_combined, engine, itertools, pd):
    links_df = df_combined.copy()

    agents_from_db = pd.read_sql('SELECT "Id" as "AgentId", "Name" FROM "Agents"', engine)

    occupations_from_db = pd.read_sql('SELECT "Id" as "OccupationId", "Title", "Area" FROM "Occupations"', engine)


    def process_row(row):
        a_list = [x.strip() for x in str(row['area']).split(',')] if pd.notnull(row['area']) else [None]
        o_list = [x.strip() for x in str(row['occupation']).split(',')] if pd.notnull(row['occupation']) else [None]
        return list(itertools.zip_longest(a_list, o_list))

    links_df['zipped'] = links_df.apply(process_row, axis=1)
    links_df = links_df.explode('zipped')

    links_df[['Area', 'Title']] = pd.DataFrame(links_df['zipped'].tolist(), index=links_df.index)
    links_df['Title'] = links_df['Title'].fillna(links_df['Area'])

    links_df['Title'] = links_df['Title'].str.strip()
    links_df['Area'] = links_df['Area'].str.strip()

    links_payload = links_df.reset_index().merge(
        agents_from_db, 
        left_on="name", 
        right_on="Name", 
        how="inner"
    )

    links_payload = links_payload.merge(
        occupations_from_db, 
        on=["Title", "Area"], 
        how="inner"
    )

    agents_occupations_final = links_payload[["AgentId", "OccupationId"]].drop_duplicates()

    agents_occupations_final.to_sql("AgentsOccupations", engine, if_exists="append", index=False, dtype={"AgentId": UUID, "OccupationId": UUID})
    return


if __name__ == "__main__":
    app.run()
