import configparser
from datetime import datetime
import dropbox
import os
from pathlib import Path
import pandas as pd
import polars as pl

dbx = dropbox.Dropbox(os.environ.get("DROPBOX"))

config = configparser.ConfigParser(inline_comment_prefixes="^")
config.read(Path.home() / "bol_export_files.ini")

file_today = max(
    (Path.home() / "for_import_basis_file").glob("**/basis_sorted_PriceList_bol*.csv"),
    key=os.path.getmtime,
)

bron_datafile = pd.read_csv(file_today, usecols=["Product ID eigen", "Inkoopprijs (excl. BTW)"])

omschrijving_basis_bestand_pd = (
    pl.read_excel(
        max((Path.home() / "ean_numbers_basisfiles").glob("basis_*.xlsm"), key=os.path.getctime),
        columns = ["Product ID eigen", "EAN", "EAN (handmatig)", "Omschrijving"],
        infer_schema_length = 0
    )
    .filter(pl.col("Product ID eigen").is_not_null())
    .rename({"EAN (handmatig)": "EAN_handmatig"})
    .to_pandas()
)

bron_data = pd.merge(bron_datafile, omschrijving_basis_bestand_pd, on="Product ID eigen", how="left")

# alle leveranciers af lopen.
leveranciers_current_prices = []
for folder in Path.home().iterdir():
    if folder.is_dir() and len(folder.stem) == 3 and folder.stem != "tmp":  # altijd 3 letter afkorting
        if folder.stem == "EXL":
            leveranciers_data = pd.read_csv(
                max(folder.glob(f"**/{folder.name}_Vendit*.csv")), usecols=["sku", "price", "end_date_promo"]
            ).assign(sku=lambda x: folder.stem + x.sku.astype(str)).rename(columns={ "price": "Inkoopprijs exclusief"})
        else:
            try:
                leveranciers_data = pd.read_csv(
                    max(folder.glob(f"**/{folder.name}_Vendit*.csv")), usecols=["sku", "Inkoopprijs exclusief"]
                ).assign(sku=lambda x: folder.stem + x.sku.astype(str))
                leveranciers_current_prices.append(leveranciers_data)
            except ValueError:
                    print(f"Geen geldige CSV gevonden in {folder.name}, overslaan.")
                    continue

leveranciers_prices_today = pd.concat(leveranciers_current_prices, ignore_index=True).rename(
    columns={"sku": "Product ID eigen", "Inkoopprijs exclusief": "nieuwe_Inkoopprijs"}
)

price_info = (
    pd.merge(bron_data, leveranciers_prices_today, on="Product ID eigen", how="left")
    .dropna(subset=["nieuwe_Inkoopprijs"])
    .rename(columns={"Product ID eigen": "Product_ID_eigen"})
)

pallet_verzendkosten = float(config.get("voor import file basisbestand", "verzendkosten_pallet_klein"))
price_info = (
    price_info.assign(
        price_difference=lambda x: (x["Inkoopprijs (excl. BTW)"] - x["nieuwe_Inkoopprijs"]).round(2),
        percentage_difference=lambda x: (x["price_difference"] / x["Inkoopprijs (excl. BTW)"]) * 100,
        EAN = lambda x: x["EAN"].str.zfill(13),
        EAN_handmatig = lambda x: x["EAN_handmatig"].str.zfill(13),
    )
    .round(2)
    .query("price_difference > `Inkoopprijs (excl. BTW)` * 0.05")
    .query("price_difference != @pallet_verzendkosten")
    .sort_values(
        by=["Product_ID_eigen", "percentage_difference"],
        key=lambda x: x if x.name != "Product_ID_eigen" else x.str[:3],
        ascending=[True, False],
    )
)

date_now = datetime.now().strftime("%c").replace(":", "-")

with pd.ExcelWriter(f"price_verschil_{date_now}.xlsx") as writer:
    price_info.to_excel(writer, sheet_name="price_verschil", index=False,freeze_panes=(1,0))
    worksheet = writer.sheets['price_verschil']
    worksheet.set_landscape()
    worksheet.set_column('A:A', 20)
    worksheet.set_column('B:B', 20)
    worksheet.set_column('C:D', 17)
    worksheet.set_column('E:E', 90)
    worksheet.set_column('F:F', 15)
    worksheet.set_column('G:I', 20)

latest_file = max(Path.cwd().glob("price_verschil_*.xlsx"), key=os.path.getctime)
with open(latest_file, "rb") as f:
    dbx.files_upload(
        f.read(), "/gebruikers/peter/" + latest_file.name, mode=dropbox.files.WriteMode("overwrite", None), mute=True
    )
