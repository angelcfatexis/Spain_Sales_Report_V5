from kedro.pipeline import Pipeline, node, pipeline

from .nodes import *


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=conversion_to_polars_file1,
                #inputs="file_1",
                inputs = None,
                outputs="df1_polars",
                name="conversion_to_polars_file1_node",
            ),
            node(
                func=conversion_to_polars_file2,
                #inputs="file_2",
                inputs = None,
                outputs="df2_polars",
                name="conversion_to_polars_file2_node",
            ),
            node(
                func=filter_overall_over_60_file1,
                inputs="df1_polars",
                outputs="df1_filter_overall_over_60",
                name="filter_overall_over_60_file1_node",
            ),
            node(
                func=sort_by_short_name_file1,
                inputs="df1_filter_overall_over_60",
                outputs="df1_sorted_by_short_name",
                name="sort_by_short_name_file1_node",
            ),
            node(
                func=sort_by_potential_file2,
                inputs="df2_polars",
                outputs="df2_sorted_by_potential",
                name="sort_by_potential_file2_node",
            ),
            node(
                func=creation_of_cross_file2,
                inputs="df2_sorted_by_potential",
                outputs="df2_cross_created",
                name="creation_of_cross_file2_node",
            ),
            node(
                func=groupby_short_name_file2,
                inputs="df2_cross_created",
                outputs="df2_groupby_short_name",
                name="groupby_short_name_file2_node",
            ),
            node(
                func=inner_join_over_short_name,
                inputs=['df1_sorted_by_short_name', 'df2_groupby_short_name'],
                outputs="df_joined_1",
                name="inner_join_over_short_name_node",
            ),
            node(
                func=filter_potential_over_66_file1,
                inputs="df_joined_1",
                outputs="df_joined_1_filtered_potential",
                name="filter_potential_over_66_file1_node",
            ),
            node(
                func=group_by_shor_tname_joined_data,
                inputs="df_joined_1_filtered_potential",
                outputs="df_joined_1_groupby_short_name",
                name="group_by_shor_tname_joined_data_node",
            ),
            node(
                func=sort_by_short_name_joined_data,
                inputs="df_joined_1_groupby_short_name",
                outputs="df_joined_1_sortby_short_name",
                name="sort_by_short_name_joined_data_node",
            ),

        ]
    )