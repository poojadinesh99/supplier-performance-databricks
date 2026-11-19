-- Queries behind the "Lieferantenleistung im Überblick" dashboard.
-- Source: Delta table workspace.default.supplier_performance_gold
-- (written by notebooks/Supplier_Performance.py)

-- KPI tiles ------------------------------------------------------------------
-- Lieferfüllgrad (%), Ø Verspätung (Tage) and Termintreue (%) are unweighted
-- averages over the 12 suppliers.
SELECT
  AVG(`Lieferfüllgrad`)                    AS lieferfuellgrad,
  AVG(`Durchschnittliche_Verspätung_Tage`) AS avg_verspaetung_tage,
  AVG(Termintreue)                         AS termintreue
FROM workspace.default.supplier_performance_gold;

-- Bar chart: Ø Verspätung pro Lieferant --------------------------------------
SELECT
  Lieferant_Name,
  AVG(`Durchschnittliche_Verspätung_Tage`) AS avg_verspaetung_tage
FROM workspace.default.supplier_performance_gold
GROUP BY Lieferant_Name
ORDER BY avg_verspaetung_tage DESC;

-- Table: Lieferantenleistung je Lieferant (dataset "lieferanten_tabelle") ----
SELECT
  Lieferant_Name,
  Lieferantenbewertung,
  Termintreue,
  `Lieferfüllgrad`,
  `Durchschnittliche_Verspätung_Tage`
FROM workspace.default.supplier_performance_gold
ORDER BY `Durchschnittliche_Verspätung_Tage` DESC;

-- Pie chart: Lieferantenanzahl pro Bewertung ---------------------------------
SELECT
  Lieferantenbewertung,
  COUNT(Lieferant_Name) AS anzahl_lieferanten
FROM workspace.default.supplier_performance_gold
GROUP BY Lieferantenbewertung;
