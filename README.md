﻿## Autonomous ETL Using Zero Engineering Framework for E-commerce Platforms
This project implements a Zero ETL framework that fully automates the ETL (Extract, Transform, Load) process for e-commerce data pipelines. The system is designed to minimize repetitive tasks and reduce the dependency on manual coding by enabling operations through a simple, user-friendly interface. Users can perform complex data operations such as schema mapping, transformations, and visualizations using pre-configured workflows and natural language prompts.

The platform leverages a medallion architecture (Raw, Silver, and Golden layers) to structure data across the pipeline stages. Advanced features like LLM-driven query generation and Word2Vec-based schema mapping further enhance flexibility and reduce engineering overhead.

### Key Features
#### Zero-Code Interface
Enables users to manage the entire ETL pipeline—data extraction, transformation, and loading—through a graphical interface without writing any code.

#### Natural Language Querying (Text-to-SQL)
Empowers users to filter and aggregate data using everyday language via a fine-tuned T5-small model, which converts prompts into executable SQL queries.

#### Automated Schema Mapping
Utilizes Word2Vec embeddings to intelligently map and align schemas between disparate datasets, significantly reducing manual intervention.

#### Medallion Architecture Implementation

- Raw Layer: Captures data via full or incremental extraction processes.

- Silver Layer: Hosts cleaned and transformed data, ready for further processing.

- Golden Layer: Contains refined, business-ready datasets produced via LLM-guided transformations.

#### Data Visualization Module
Facilitates interactive analysis of Golden Layer outputs through integrated dashboards and plotting capabilities.

### Core Modules
**Data Extraction**
Supports both full and incremental data loads, storing outputs in the Raw Layer.

**Schema Mapping**
Applies automated column matching using Word2Vec, reducing manual mapping efforts.

**Transformation & Aggregation**
Offers customizable transformation options through the UI and LLM-based logic.

**Natural Language Interface**
Allows users to specify custom aggregation and filtering requirements using simple text inputs.

**Visualization**
Displays the final business insights from the Golden Layer using intuitive visual tools.

### Technology Stack
- Programming & Data Tools: Python, Pandas, MySQL

- User Interface: Streamlit

- Machine Learning: TensorFlow, T5-small (LLM)

- NLP & Semantic Matching: Word2Vec

- Domain Skills: Data Engineering, Data Modeling, Natural Language Processing (NLP)


### Credits
This project was developed by:

- Karol Monsy Theruvil @Karolmonsy

- M S Devanarayan @msdnarayan

- Siona Mariam Thomas @si0na

- Vishnu Narayanan @vxshnu
