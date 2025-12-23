-- CREATE DATABASE "dbd-25";
-- Подключаем расширение для uuid
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- Сначала создаем таблицы в которых нет явных связей, чтобы не было ошибок при создании таблиц
-- Law
CREATE TABLE "Laws" (
    "Type" VARCHAR(10) PRIMARY KEY,
    "Title" TEXT NOT NULL,
    "StartDate" DATE NOT NULL,
    "EndDate" DATE,
    CONSTRAINT unique_laws_type_title_start_date UNIQUE ("Type", "Title", "StartDate"),
    CONSTRAINT check_law_dates CHECK (
        "EndDate" IS NULL
        OR "EndDate" >= "StartDate"
    )
);
-- Regions
CREATE TABLE "Regions" (
    "Id" VARCHAR(3) PRIMARY KEY,
    "Name" TEXT NOT NULL UNIQUE
);
-- Judges
CREATE TABLE "Judges" (
    "Id" UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "Name" TEXT NOT NULL
);
-- Occupations
CREATE TABLE "Occupations" (
    "Id" UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "Title" TEXT NOT NULL,
    "Area" TEXT,
    CONSTRAINT unique_occupation_area UNIQUE ("Title", "Area")
);
-- Articles
-- Constraint гарантирует, что при переименовании или удалении типа закона (например, "Уголовного") обрабатываются все связанные с ним статьи.
CREATE TABLE "Articles" (
    "Number" VARCHAR(10) NOT NULL,
    "LawType" VARCHAR(10) NOT NULL,
    "Name" TEXT NOT NULL,
    PRIMARY KEY ("Number", "LawType"),
    FOREIGN KEY ("LawType") REFERENCES "Laws"("Type") ON DELETE CASCADE ON UPDATE CASCADE
);
-- Cases
CREATE TABLE "Cases" (
    "Id" UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "CaseNumber" TEXT NOT NULL UNIQUE
);
CREATE INDEX "idx_cases_casenumber" ON "Cases" ("CaseNumber");
-- Cases_Articles
-- Constraint автоматически устраняется связь между законами и судебными делами, когда устраняется любая из сторон.
CREATE TABLE "CasesArticles" (
    "ArticleNumber" VARCHAR(10) NOT NULL,
    "ArticleLawType" VARCHAR(10) NOT NULL,
    "CaseId" UUID NOT NULL,
    PRIMARY KEY ("ArticleNumber", "ArticleLawType", "CaseId"),
    FOREIGN KEY ("ArticleNumber", "ArticleLawType") REFERENCES "Articles"("Number", "LawType") ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY ("CaseId") REFERENCES "Cases"("Id") ON DELETE CASCADE
);
-- Courts
CREATE TABLE "Courts" (
    "Id" UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "Name" TEXT NOT NULL,
    "RegionId" VARCHAR(3) NOT NULL,
    FOREIGN KEY ("RegionId") REFERENCES "Regions"("Id"),
    CONSTRAINT unique_courts_name_region_id UNIQUE ("Name", "RegionId")
);
CREATE INDEX "idx_courts_regionid" ON "Courts" ("RegionId");
-- Courts_Cases
-- Constraint удаляет историю обращений/запись экземпляра, если удалена центральная запись обращения.
CREATE TABLE "CourtsCases" (
    "CourtId" UUID NOT NULL,
    "CaseId" UUID NOT NULL,
    "InstanceLevel" TEXT NOT NULL,
    "EntryDate" DATE NOT NULL,
    "DecisionDate" DATE,
    "Decision" TEXT,
    PRIMARY KEY ("CourtId", "CaseId"),
    FOREIGN KEY ("CourtId") REFERENCES "Courts"("Id") ON DELETE CASCADE,
    FOREIGN KEY ("CaseId") REFERENCES "Cases"("Id") ON DELETE CASCADE
);
-- Judges_Cases
CREATE TABLE "JudgesCases" (
    "JudgeId" UUID NOT NULL,
    "CaseId" UUID NOT NULL,
    PRIMARY KEY ("JudgeId", "CaseId"),
    FOREIGN KEY ("JudgeId") REFERENCES "Judges"("Id"),
    FOREIGN KEY ("CaseId") REFERENCES "Cases"("Id") ON DELETE CASCADE
);
-- Agents
CREATE TABLE "Agents" (
    "Id" UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "Name" TEXT NOT NULL,
    "NumberFromMinyst" VARCHAR(8) NOT NULL,
    "Type" TEXT NOT NULL,
    "StartDate" DATE NOT NULL,
    "EndDate" DATE,
    CONSTRAINT check_agent_dates CHECK (
        "EndDate" IS NULL
        OR "EndDate" >= "StartDate"
    ),
    CONSTRAINT unique_name_number_from_minyst_type UNIQUE ("Name", "NumberFromMinyst", "Type")
);
-- Agents_Cases (исправлено имя таблицы, было JudgesCases)
CREATE TABLE "AgentsCases" (
    "AgentId" UUID NOT NULL,
    "CaseId" UUID NOT NULL,
    PRIMARY KEY ("AgentId", "CaseId"),
    FOREIGN KEY ("AgentId") REFERENCES "Agents"("Id"),
    FOREIGN KEY ("CaseId") REFERENCES "Cases"("Id") ON DELETE CASCADE
);
-- Agents_Occupations
CREATE TABLE "AgentsOccupations" (
    "AgentId" UUID NOT NULL,
    "OccupationId" UUID NOT NULL,
    PRIMARY KEY ("AgentId", "OccupationId"),
    FOREIGN KEY ("AgentId") REFERENCES "Agents"("Id"),
    FOREIGN KEY ("OccupationId") REFERENCES "Occupations"("Id")
);