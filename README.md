# ppp-enrichment

Scaffold for a Python 3.11 PPP/EIDL enrichment pipeline.

## Layout

- `src/ppp_enrichment/`: pipeline modules and shared configuration.
- `data/input/`: raw PPP/EIDL CSV inputs (user-provided).
- `data/output/`: enriched exports.
- `logs/`: runtime log files.
- `config/`: local configuration support files like `.env`.



You already did the right Cursor prompts; now you just need to understand the **runtime commands** and then bake them into the README so you don’t have to think.

Assuming Cursor created `verify_env.py`, `run_pipeline.py`, and `requirements.txt` as we discussed, here’s how to actually run them and what to put into the README.

***

## 1. How to run `verify_env` and the pipeline

From your project root (`ppp-enrichment`), with your venv activated:

### A. Verify environment

```bash
python -m src.ppp_enrichment.verify_env
```

This should:

- Check Python version.  
- Import `pandas`, `httpx`, `bs4`, `lxml`, `ddgs`, etc.  
- Import your project modules.  
- Check if `data/input/ppp-war.csv` exists.

You’ll see messages like:

- `Environment OK: Python version, dependencies, and project modules loaded successfully.`  
- `PPP raw file found at ...: True/False`

If PPP raw is missing, fix that (download SBA PPP FOIA CSV and save as `data/input/ppp-war.csv`) before running anything else.

### B. Run a small test

```bash
python -m src.ppp_enrichment.run_pipeline --leads 20
```

This will:

- Take a subset of borrowers from `ppp-war.csv`.  
- Resolve domains.  
- Crawl, extract names/emails/phones.  
- Delete used rows from `ppp-war.csv` if you implemented that logic.  
- Write outputs under something like:

  - `data/output/Data_20_YYYYMMDD/`  

Inside that folder you’ll see:

- `borrowers_base_sample.csv`  
- `borrowers_with_domains.csv`  
- `enriched_borrowers.csv`  
- `Vaishnavi_Clean_<clean_count>_1.csv`  (your clean leads file)

### C. Run a normal lead-gen job

```bash
python -m src.ppp_enrichment.run_pipeline --leads 500
```

Same as above, but for a larger sample; output goes to:

- `data/output/Data_500_YYYYMMDD/`  

and the final clean file will be:

- `Vaishnavi_Clean_<actual_clean_leads>_1.csv` (actual may be less than 500)

***

## 2. What to add to README (high-level)

You want anyone (including future you) to know:

- How to set up the machine.  
- How to verify.  
- How to run test + real jobs.  
- Where results live.

Here’s the structure you want README to have:

1. **Overview**  
   Short description of what the project does.

2. **Prerequisites**  
   - Python 3.9+  
   - Git

3. **Initial computer setup**

   ```bash
   # Clone repo
   git clone <YOUR_GITHUB_URL> ppp-enrichment
   cd ppp-enrichment

   # Create and activate virtualenv
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1   # Windows
   # source .venv/bin/activate    # macOS/Linux

   # Install dependencies
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   ```

4. **Download PPP raw data**

   - Go to SBA PPP FOIA dataset page. [data.sba](https://data.sba.gov/en/dataset/ppp-foia)
   - Download a CSV.  
   - Save it as:

     ```text
     data/input/ppp-war.csv
     ```

   - Note that this file is the working raw dataset; each pipeline run may shrink it by removing processed borrowers (if you implemented that behavior).

5. **Verify environment (first run)**

   ```bash
   python -m src.ppp_enrichment.verify_env
   ```

   - If this says PPP raw is missing, fix step 4.  
   - If OK, move on.

6. **How the codebase works (high level)**

   - `ingest.py` – reads PPP raw CSV and builds borrower base.  
   - `domains.py` – resolves company → website via DuckDuckGo.  
   - `crawler.py` – fetches a few pages per domain (home/about/contact).  
   - `extract.py` – pulls names/emails/phones out of HTML.  
   - `rules.py` – picks best contact or falls back to synthetic name + generic email.  
   - `run_pipeline.py` – orchestration script:
     - Samples borrowers from raw.  
     - Resolves domains + enriches contacts.  
     - Deletes used rows from raw (optional).  
     - Writes all outputs for that run into `data/output/Data_<leads>_<date>/`.  
     - Writes final clean leads file with 6 columns.

7. **Initial test run**

   ```bash
   python -m src.ppp_enrichment.run_pipeline --leads 20
   ```

   - Verify:
     - Folder `data/output/Data_20_YYYYMMDD/` appears.  
     - `Vaishnavi_Clean_<N>_1.csv` exists inside and opens in Excel with:

       - First Name  
       - Second Name  
       - Email Address  
       - Phone Number  
       - Company Name  
       - Company URL

8. **Lead generation run (normal usage)**

   ```bash
   python -m src.ppp_enrichment.run_pipeline --leads 500
   ```

   - Same behavior, bigger sample.  
   - Look at `data/output/Data_500_YYYYMMDD/` and `Vaishnavi_Clean_<N>_1.csv`.

9. **Outputs and logs**

   - `data/output/Data_<leads>_<date>/` – per-run outputs.  
   - `logs/` – per-run log files (names like `enrich_sample_YYYYMMDD_HHMMSS.log`).

10. **Updating PPP data**

    - When raw PPP is exhausted or when you want new data:
      - Replace `data/input/ppp-war.csv` with a fresh SBA PPP CSV.  
      - Optionally archive old `data/output` / `logs`.  
      - Rerun `verify_env` and then `run_pipeline`.

If you want, you can copy/paste pieces of this directly into README, but you’ve already got Cursor modifying README for you; you can now ask Cursor:

> Update `README.md` to include:
> - The command `python -m src.ppp_enrichment.verify_env` as the environment verification step.
> - The initial test run command: `python -m src.ppp_enrichment.run_pipeline --leads 20`.
> - The normal lead gen run command: `python -m src.ppp_enrichment.run_pipeline --leads 500`.
> - A short explanation of what `run_pipeline` does and where per-run outputs are saved (`data/output/Data_<leads>_<YYYYMMDD>/` and `Vaishnavi_Clean_<N>_1.csv`).

But from a runtime standpoint, for you, the commands are:

- Verify: `python -m src.ppp_enrichment.verify_env`  
- Test: `python -m src.ppp_enrichment.run_pipeline --leads 20`  
- Real: `python -m src.ppp_enrichment.run_pipeline --leads <whatever>`