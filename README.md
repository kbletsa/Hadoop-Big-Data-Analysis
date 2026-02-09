# Big Data Analysis Technologies - Hadoop

[cite_start]This repository contains the implementation of the 2nd Assignment for the "Big Data Analysis Technologies" course[cite: 56]. The project focuses on processing large datasets using **Hadoop MapReduce** framework in Java.

## Project Overview

The assignment consists of two MapReduce tasks:

### 1. Numeronym Counter
* [cite_start]**Goal:** Generate "numeronyms" for words in a text dataset and filter them based on a minimum frequency threshold ($k$)[cite: 58, 62].
* [cite_start]**Definition:** A numeronym is formed by the first letter, the count of distinct letters between the first and last, and the last letter (e.g., "internationalization" $\rightarrow$ "i18n")[cite: 59].
* **Implementation Logic:**
  * [cite_start]**Mapper:** Tokenizes text, removes non-alphabetic characters, converts to lowercase, and ignores words with length $< 3$[cite: 60, 61].
  * [cite_start]**Reducer:** Sums the occurrences and filters out results appearing fewer than $k$ times[cite: 62].
* **File:** `NumeronymCount.java`

### 2. DNA N-Gram Analysis
* [cite_start]**Goal:** Analyze biological data (E. coli DNA sequence) to count the frequency of 2-grams, 3-grams, and 4-grams[cite: 66, 68].
* **Implementation Logic:**
  * [cite_start]**Mapper:** Uses a **sliding window** approach to extract substrings of length 2, 3, and 4 from each line[cite: 69].
  * [cite_start]**Reducer:** Aggregates counts for each unique N-gram[cite: 70].
  * [cite_start]**Result:** Identifying common patterns like "GC" (most frequent 2-gram) or "CGC".
* **File:** `DnaCount.java`

---

## How to Run

### Prerequisites
* Java Development Kit (JDK)
* Apache Hadoop (HDFS & MapReduce)

### Execution Steps

#### 1. Numeronym Task
The program requires an input path, an output path, and an integer $k$ (minimum frequency threshold).

```bash
# Compile
javac -classpath `hadoop classpath` NumeronymCount.java -d classes/
jar -cvf numeronym.jar -C classes/ .

# Run (Example with k=10)
hadoop jar numeronym.jar NumeronymCount /input/path /output/path 10
