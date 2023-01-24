
base_url = 'https://lido.addepar.com/api/v1/jobs'

header = {
    "Accept": "application/vnd.api+json",
    "Addepar-Firm": "222",
    "Content-Type": "application/vnd.api+json"
}

holdings = {
  "import_proc": "dbimport.usp_ImportAddeparHoldings",
  "post_import_proc": "Addepar.usp_HoldingsPostImport",
  "params": """
  {
    "data":{
      "type":"job",
      "attributes":{
        "job_type":"PORTFOLIO_QUERY",
        "parameters":{
          "columns":[
            {"key": "_custom_account_289300"},
            {"key": "_custom_symbol_260903"},
            {"key": "cusip"},
            {
              "key": "shares",
              "arguments": {
                "time_point": "current"
              }
            },
            {
              "key": "price_per_share",
              "arguments": {
                "time_point": "current",
                "use_percent_of_original_principal": false,
                "currency": "USD"
              }
            },
            {
              "key": "value",
                "arguments": {
                "time_point": "current",
                "accrued": "none",
                "negative_value_handling": "show_as_negative",
                "valuation_adjustment_type": "none",
                "currency": "USD"
              }
            },
            {"key": "_custom_flyer_asset_class_605825"},
            {"key": "holding_end_date"},
            {"key": "purchase_date"}
          ],
          "groupings": [
            {"key": "_custom_account_289300"},
            {"key": "security"}
          ],
          "filters":[],
          "portfolio_type":"firm",
          "portfolio_id":[1],
          "start_date": "__StartDate__",
          "end_date": "__EndDate__",
          "hide_previous_holdings": true,
          "group_by_historical_values": false,
          "group_by_multiple_attribute_values": false,
          "look_through_composite_securities": false,
          "display_account_fees": false,
          "disable_total_row": false,
          "short_held_asset_display_preference": "inherit"
        }
      }
    }
  }"""
}

accounts = {
  "import_proc": "dbimport.usp_ImportAddeparAccounts",
  "post_import_proc": "Addepar.usp_AccountsPostImport",
  "params": """
  {
    "data": {
      "type": "job",
      "attributes": {
        "job_type":"PORTFOLIO_QUERY",
        "parameters":{
          "columns": [
            {"key": "_custom_account_289300"},
            {"key": "top_level_owner"},
            {"key": "_custom_registration_298373"},
            {
              "key": "value",
              "arguments": {
                "time_point": "current",
                "accrued": "all",
                "negative_value_handling": "show_as_negative",
                "valuation_adjustment_type": "none",
                "currency": "USD"
              }
            },
            {"key": "_custom_lido_advisor_178227"},
            {"key": "financial_service"},
            {"key": "_custom_erisa_pooled_plan_698408"},
            {"key": "_custom_discretionary_298374"},
            {"key": "_custom_risk_profile_325712"},
            {"key": "_custom_rp_date_334786"},
            {"key": "billing_schedule"},
            {"key": "_custom_total_net_worth_lido_328933"},
            {"key": "_custom_total_net_worth_lido2_328946"},
            {"key": "_custom_oaccount_272353"}
          ],
          "groupings": [
            {
              "key": "direct_owner"
            }
          ],
          "filters": [
            {
              "attribute": {
                "key": "financial_service"
              },
              "type": "discrete",
              "operator": "exclude",
              "values": [
                "interactivebrokerscustodianservice"
              ],
              "unassigned_account_fees": false
            },
            {
              "attribute": {
                "key": "ownership"
              },
              "type": "discrete",
              "operator": "exclude",
              "values": [
                "Absolute Return Model Account (37112110)",
                "Adam Smith",
                "Adam Smith 401k (5794676)",
                "Adam Smith Individual Trust",
                "Adam Smith IRA (786581)",
                "Adam Smith Irrevocable Trust",
                "AI Fixed Income Model Account (676601339)",
                "DT Simpson Relationship",
                "Fast Model - Anish Ramachandran Simple IRA (676666744)",
                "Fixed Income Model Account (74544839)",
                "Focused Growth Model Account (83742085)",
                "Global Growth Model Account (JC IRA) (676601340)",
                "Lido Cap And Cushion Fund (Institutional), LP",
                "Lido Cap And Cushion Fund (Institutional), LP (Cash Account)",
                "Lido Cap And Cushion Fund (Institutional), LP (Cash Account) (73675547)",
                "Lido Cap And Cushion Fund (Institutional), LP (Cash Account) (U3016537)",
                "Lido Cap And Cushion Fund (Institutional), LP (Trading Account)",
                "Lido Cap And Cushion Fund (Institutional), LP (Trading Account) (18526797)",
                "Lido Cap And Cushion Fund (Institutional), LP (Trading Account) (U3036344)",
                "Lido Cap And Cushion Fund, LP",
                "Lido Cap And Cushion Fund, LP (Cash Account)",
                "Lido Cap And Cushion Fund, LP (Cash Account) (64781596)",
                "Lido Cap And Cushion Fund, LP (Cash Account) (U3016365)",
                "Lido Cap And Cushion Fund, LP (Trading Account)",
                "Lido Cap And Cushion Fund, LP (Trading Account) (16092056)",
                "Lido Cap And Cushion Fund, LP (Trading Account) (U3036336)",
                "Lido Strats",
                "Model Client",
                "Multi Asset Model - Anish Ramachandran Traditional IRA (645422228)",
                "NonLC Model Account (68945308)",
                "Opp Bond Corp Model (656387235)",
                "Opp Bond Muni Nat Model (656387236)",
                "Opportunistic Income Model Account (43798117)",
                "Sector Rotation Model Account (63847312)",
                "Tactical Model Account (68050873)",
                "Unconstrained Model Account (84383416)",
                "Weighted Sector Model Account (676601347)"
              ],
              "unassigned_account_fees": false
            }
          ],
          "portfolio_type": "firm",
          "portfolio_id": [1],
          "start_date": "__StartDate__",
          "end_date": "__EndDate__",
          "hide_previous_holdings": true,
          "group_by_historical_values": false,
          "group_by_multiple_attribute_values": false,
          "look_through_composite_securities": false,
          "display_account_fees": false,
          "disable_total_row": false,
          "short_held_asset_display_preference": "inherit"
        }
      }
    }
  }"""
}