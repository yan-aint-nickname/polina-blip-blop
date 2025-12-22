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
    insert_query = text("""
        INSERT INTO "Occupations" ("Title", "Area")
        VALUES (:title, :area)
        ON CONFLICT ("Title", "Area") DO NOTHING;
    """)


    with engine.connect() as conn:
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
                    conn.execute(insert_query, {"title": title.strip(), "area": area.strip() if area else None})
                except Exception as e:
                    print(e)
        conn.commit()
    return (conn,)


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

    with engine.connect() as conn:
        conn.execute(text(f'INSERT INTO "Laws" ("Type", "Title", "StartDate") VALUES (:type, :title, :start_date)'), laws_data)
        conn.commit()
    return (conn,)


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
def _(conn, text):
    # Проверяем кол-во регионов
    regions_from_db = conn.execute(text('select * from "Regions"')).fetchall()
    print(len(regions_from_db))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Обрабатываем судебные дела
    """)
    return


@app.cell
def _(pd):
    cases_parsed = pd.read_csv("../data/cases_parsed.csv", sep=";")
    # cases_parsed
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
    full_names = pd.DataFrame({"Name": cases_parsed["judge"].unique()})
    print("Кол-во судей", len(full_names))

    full_names.to_sql(name="Judges", con=engine, if_exists="append", index=False)
    return


@app.cell
def _(conn, text):
    # Проверяем кол-во судей в бд
    judges_from_db = conn.execute(text('select * from "Judges"')).fetchall()
    print(len(judges_from_db))
    return (judges_from_db,)


@app.cell
def _(mo):
    mo.md(r"""
    #### Обрабатываем суды(здания) Courts
    """)
    return


@app.cell
def _(cases_parsed, engine):
    unique_courts = cases_parsed["court_name"].unique()
    print("Кол-во судов", len(unique_courts))

    courtes_parsed_to_db = cases_parsed[["court_name", "region"]].copy()
    courtes_parsed_to_db.columns = ["Name", "RegionId"]

    courtes_parsed_to_db.to_sql(
        name="Courts", con=engine, if_exists="append", index=False
    )
    return


@app.cell
def _(engine, text):
    # Проверяем кол-во судов в бд
    with engine.connect() as ccourtes:
        courtes_from_db = ccourtes.execute(text('select * from "Courts"')).fetchall()
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
    cases = pd.DataFrame({"CaseNumber": cases_parsed["cui"].unique()})
    print("Кол-во дел", len(cases))

    cases.to_sql(name="Cases", con=engine, if_exists="append", index=False)
    return


@app.cell
def _(engine, text):
    # Проверяем кол-во дел в бд
    with engine.connect() as ccfd:
        cases_from_db = ccfd.execute(text('select * from "Cases"')).fetchall()
    print(len(cases_from_db))
    return (cases_from_db,)


@app.cell
def _(mo):
    mo.md(r"""
    #### Обрабатываем Judges Cases
    """)
    return


@app.cell
def _(cases_from_db, cases_parsed, engine, judges_from_db, text):
    # Сохраняем судей из csv в бд

    # найти все дела в бд
    # найти судью из дела по csv и достать id судьи
    # добавить id судьи и id дела в JudgesCases
    cases_from_db_dict = {uuid: cui for uuid, cui in cases_from_db}

    cui_judge_dict = cases_parsed.set_index("cui")["judge"].to_dict()
    cui_judge_dict = (
        cases_parsed[cases_parsed["cui"].isin(cases_from_db_dict.values())]
        .set_index("cui")["judge"]
        .to_dict()
    )

    # get caseid from db by inverting cases_from_db_dict
    # 03RS0063-01-2018-003169-14: 27803044-6036-430b-a796-c2550496425f
    cui_case_id_map = {cui: uuid for uuid, cui in cases_from_db_dict.items()}
    assert len(cui_case_id_map) == len(cases_from_db_dict)

    #  Карипов Р.Г.: bd41a5e3-19b5-46d6-9c97-fce664f30b52
    judges_from_db_dict = {name: uuid for uuid, name in judges_from_db}

    # "01RS0007-01-2021-001052-22":96604cd5-6378-4170-bf15-2a3f039304cb
    cui_judge_id_map = {
        cui: judges_from_db_dict[judge] for cui, judge in cui_judge_dict.items()
    }

    case_id_judge_id_map = {
        cui_case_id_map[cui]: judge_id for cui, judge_id in cui_judge_id_map.items()
    }

    # Вставляем в бд
    values_to_insert = [
        {"judge_id": j_id, "case_id": c_id}
        for c_id, j_id in case_id_judge_id_map.items()
    ]

    insert_query = text("""
        INSERT INTO "JudgesCases" ("JudgeId", "CaseId")
        VALUES (:judge_id, :case_id)
        ON CONFLICT DO NOTHING;
    """)

    with engine.connect() as cjc:
        try:
            if values_to_insert:
                cjc.execute(insert_query, values_to_insert)
                cjc.commit()  # Важно зафиксировать изменения
                print(f"Успешно связано записей: {len(values_to_insert)}")
            else:
                print("Нет данных для вставки")
        except Exception as e:
            # Обязательная обработка ошибок для защиты от штрафа
            print(f"Ошибка при заполнении таблицы JudgesCases: {e}")
            cjc.rollback()
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Добавляем CasesArticles
    """)
    return


@app.cell
def _(cases_parsed, engine, pd):
    import re
    from sqlalchemy.dialects.postgresql import UUID

    df_cases_from_db = pd.read_sql('SELECT * FROM "Cases"', engine)
    # df_articles_from_db = pd.read_sql('SELECT * FROM "Articles"', engine)

    df_cases_parsed_copy = cases_parsed.copy()

    df_with_uuids = df_cases_parsed_copy.merge(
        df_cases_from_db[['Id', 'CaseNumber']], 
        left_on='cui', 
        right_on='CaseNumber'
    )

    pattern = r"ст\.(\d+)(?!\s*ч\.)\.(\d+)"

    def extract_articles(text):
        if not isinstance(text, str):
            return []

        matches = re.findall(pattern, text)

        return {f"{m[0]}.{m[1]}" for m in matches}

    df_with_uuids['articles'] = df_with_uuids['names'].apply(extract_articles)
    df_with_uuids['law_type'] = 'УК РФ'

    df_final_to_insert = df_with_uuids.explode('articles')[['Id', 'articles', 'law_type']]

    # bad_articles = ["126.2", "166.2"]
    # df_final_to_insert = df_final_to_insert[~df_final_to_insert['articles'].isin(bad_articles)]

    df_final_to_insert.columns = ["CaseId", "ArticleNumber", "ArticleLawType"]
    df_final_to_insert.to_sql(name="CasesArticles", con=engine, if_exists="append", index=False, dtype={"CaseId": UUID})
    return


if __name__ == "__main__":
    app.run()
