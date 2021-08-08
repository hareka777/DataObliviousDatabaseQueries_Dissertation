import regex as re

class QueryParser():
    def parse(self, query):
        if 'where' in query:
            if 'join' in query:
                regular_expression = re.compile(
                    r'^select (?P<selected_fields>[\w,.\s]+) from (?P<selected_tables>[\w,\s]+) join (?P<table2>[\w\s]+) on (?P<table1>[\w\s]+).(?P<table1_column>[\w\s]+)=(?P<table2>[\w\s]+).(?P<table2_column>[\w\s]+) where (?P<where_condition>[\w,.\s<>!=]+)')
            else:
                regular_expression = re.compile(r'^select (?P<selected_fields>[\w,.\s]+) from (?P<selected_tables>[\w,\s]+) where (?P<where_condition>[\w,\s<>!=]+)')
        else:
            if 'join' in query:
                regular_expression = re.compile(r'^select (?P<selected_fields>[\w,.\s]+) from (?P<selected_tables>[\w,\s]+) join (?P<table2>[\w\s]+) on (?P<table1>[\w\s]+).(?P<table1_column>[\w\s]+)=(?P<table2>[\w\s]+).(?P<table2_column>[\w\s]+)')
            else:
                regular_expression = re.compile(r'^select (?P<selected_fields>[\w,.\s]+) from (?P<selected_tables>[\w,\s]+)')

        search_result = regular_expression.search(query)
        parsed_dict = {}
        if not search_result:
            return None
        else:
            try:
                parsed_dict['select'] = search_result.group('selected_fields')
                parsed_dict['select'] = parsed_dict['select'].replace(' ', '').split(',')

                parsed_dict['from'] = search_result.group('selected_tables')
                parsed_dict['from'] = parsed_dict['from'].replace(' ', '').split(',')

                # if there is a join condition

                '''if search_result.groups('join'):
                    parsed_dict['join'] = search_result.group('join')
                    parsed_dict['join'] = parsed_dict['join'].replace(' ', '')'''
                print(len(search_result.groups()))

                # SELECT, FROM, WHERE query
                if len(search_result.groups()) == 3:
                    parsed_dict['where'] = search_result.group('where_condition')
                    parsed_dict['where'] = parsed_dict['where'].replace(' ', '').split('and')

                # SELECT, FROM, JOIN query or SELECT, FROM, JOIN, WHERE query
                elif len(search_result.groups()) == 6 or len(search_result.groups()) == 7:
                    parsed_dict['from'].append(search_result.group('table2'))
                    parsed_dict['join_fields'] = [search_result.group('table2_column'), search_result.group('table1_column')]

                    # SELECT, FROM, JOIN, WHERE query
                    if len(search_result.groups()) == 7:
                        parsed_dict['where'] = search_result.group('where_condition')
                        parsed_dict['where'] = parsed_dict['where'].replace(' ', '').split('and')

            except:
                print('Please enter a valid database query!')

        return parsed_dict