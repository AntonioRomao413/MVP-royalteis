# ============================================================
# INGESTÃO BRONZE - ROYALTIES E PIB DO RJ
# ============================================================

from pathlib import Path
import pandas as pd


# ============================================================
# CONFIGURAÇÕES
# ============================================================

BASE_DIR = Path("/workspaces/MVP-royalteis")
INPUT_DIR = BASE_DIR / "dados"
OUTPUT_DIR = BASE_DIR / "data" / "bronze"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# FUNÇÕES
# ============================================================

def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza os nomes das colunas."""
    df = df.copy()

    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )

    return df


def ingerir_excel(nome_arquivo: str, nome_saida: str) -> pd.DataFrame:
    """Lê Excel, normaliza colunas e salva em Parquet."""

    arquivo_entrada = INPUT_DIR / nome_arquivo
    arquivo_saida = OUTPUT_DIR / nome_saida

    print(f"Lendo: {arquivo_entrada}")

    df = pd.read_excel(arquivo_entrada)

    if df.empty:
        raise ValueError(f"Arquivo vazio: {arquivo_entrada}")

    df = normalizar_colunas(df)

    df.to_parquet(
        arquivo_saida,
        index=False,
        engine="pyarrow"
    )

    print(
        f"OK | {len(df):,} registros | "
        f"{arquivo_saida}"
    )

    return df


# ============================================================
# INGESTÃO
# ============================================================

royalties_pd = ingerir_excel(
    "royalties_rj.xlsx",
    "bronze_royalties.parquet"
)

pib_pd = ingerir_excel(
    "pib_rj.xlsx",
    "bronze_pib.parquet"
)


# ============================================================
# RESUMO
# ============================================================

print("\n" + "=" * 60)
print("INGESTÃO CONCLUÍDA")
print("=" * 60)

print(f"Royalties: {len(royalties_pd):,} registros")
print(f"PIB:       {len(pib_pd):,} registros")

print("\nArquivos:")
print(OUTPUT_DIR / "bronze_royalties.parquet")
print(OUTPUT_DIR / "bronze_pib.parquet")

