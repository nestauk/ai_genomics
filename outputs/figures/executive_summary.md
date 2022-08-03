---
title: "A scientometric analysis of AI in genomics"
subtitle: "Draft incomplete report"
author:
  - "India Kerle"
  - "Juan Mateos-Garcia"
  - "Jack Rasala"
  - "George Richardson"
  - "Jack Vines"
date:
  - "Last updated 3 August 2022"
figPrefix:
  - "Figure"
  - "Figures"
tblPrefix:
  - "Table"
  - "Tables"
secPrefix:
  - "Section"
  - "Sections"
secnumdepth: 3
linkcolor: "blue"
number_sections: true
---

# Executive Summary

We describe our data collection and processing activities in the first project epic. This includes collecting, processing and exploring data about:

- Research publications from OpenAlex
- Patents from the Google patent dataset
- Technology organisations from CrunchBase
- UKRI funded research projects from the Gateway to Research

Our analysis has so far focused on exploring different strategies to extract AI and genomics entities (papers, patents, organisations) from the data rather than generating results - this will be the focus for the second epic.

**Some highlights**

- In the data that we have collected so far we have identified:
  - 9,248 AI and genomics / genetics papers. This might be missing bona fide AI / genomics papers that haven't been labelled with those categories with OpenAlex. We are expanding our data collection to address that as a next step.
  - 14,000 AI and genetics patents (this is likely to include a number of irrelevant patents related to genetics / biomedical research) that we can remove in downstream analysis
  - Just under 200 technology organisations
  - We have yet to identify AI and genomics projects in the Gateway to Research data. In order to do this, we will draw on a strategy developed in another Nesta project to map the AI sector, and use project labels and abstracts to find genomics projects.
