# Databricks notebook source
### CAN BE FURTHER BROKE DOWN
def get_list_valid_pages_and_n_valid_pages():
  lp('Pages tests:')
  ### Results from data load
  #lp(f'We are expecting: {int_total_number_of_pages} Total pages observed')
  lp(f'We got          : {len(list_all_pages)} Total pages observed')

  ### Some responses contain empty lists, so we need to separate valid responses
  list_valid_pages = [page_i for page_i in list_all_pages if len(page_i) > 0]
  list_empty_pages = [page_i for page_i in list_all_pages if len(page_i) == 0]
  n_valid_pages = len(list_valid_pages)
  n_empty_pages = len(list_empty_pages)

  lp(f'Out of those    : n_valid_pages: {n_valid_pages}')
  lp(f'Out of those    : n_empty_pages: {n_empty_pages}')


  lp('Rows/elements tests:')
  n_rows_observed = 0
  for i, valid_page_i in enumerate(list_valid_pages):
    len_valid_page_i = len(valid_page_i)
    n_rows_observed = n_rows_observed + len_valid_page_i

  #lp(f'We are expecting: {int_total_number_of_rows} Total rows/elements observed')
  lp(f'We got          : {n_rows_observed} Total rows/elements observed')
  
  return list_valid_pages, n_valid_pages
