## A simple package to manage CRSP and Compustat Data.

The package is currently setup to work with the merged CRSP and Compustat dataset. However, it is simple to add new
collections to the database.

* Inserting data [example](https://github.com/Alexd14/equity-db/blob/main/equity_db/examples/read_example.ipynb)
* Reading data [example](https://github.com/Alexd14/equity-db/blob/main/equity_db/examples/inserting_example.ipynb)

## Schema Design (example variables)

```json
{
  lpermno: "14593",
  tic: "AAPL",
  weburl: "www.apple.com",
  spcseccd: "940",
  fic: "USA",
  add1: "One Apple Park Way",
  linkenddt: "E",
  phone: "408-996-1010",
  addzip: "95014",
  busdesc: "Apple Inc. designs, manufactures, and markets smartphones, personal...",
  gvkey: "001690",
  gsector: "45",
  cusip: "037833100",
  loc: "USA",
  liid: "01",
  linkdt: 1980-12-12,
  tpci: "0",
  cik: "0000320193",
  ein: "94-2404110",
  gsubind: "45202030",
  state: "CA",
  city: "Cupertino",
  timeseries: [
    {
      datadate: 1984-12-26,
      curcdd: "USD",
      ajexdi: 224,
      prcstd: 3,
      prchd: 27.875,
      prcld: 27.375,
      trfd: 1,
    },
    {
      datadate: 1990-12-27,
      curcdd: "USD",
      ajexdi: 224,
      prcstd: 3,
      prchd: 29.234,
      prcld: 29.101,
      trfd: 1,
    },
  ],
}
```

