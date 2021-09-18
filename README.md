# Сбор и анализ вакансий с hh.ru
---
**Идея:** проект направлен на помощь в сборе и анализе вакансий на hh.ru. Сбор вакансий осуществляется через API hh.ru.

**Содержание:**
* data_collector.py содержит класс DataCollector, способный получать вакансии, соответствующие запросу, и сохранять их в виде .csv.
* jobs_analysis.ipynb содержит анализ полученных вакансий, он включает:
    * Выявление наиболее популярных ключевых навыков, обозначенных в вакансиях
    * График распределения зарплат полученных вакансий
    * Вычисление надбавки к зарплате за опыт работы
    * и другое