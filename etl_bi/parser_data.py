def parser_row_parentheses(row):
    return row if not row == '()' else None


def parser_row_empty(row):
    return row if not row == '' else None
