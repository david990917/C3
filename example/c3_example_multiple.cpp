#include "c3/C3Compressor.hpp"

int main(int argc, char** argv) {

	c3::C3Compressor compressor;
	//    std::vector<c3_bench::Dataset> datasets = c3_bench::datasets_public_bi_small;
	std::vector<c3_bench::Dataset> datasets = c3_bench::hanwen_datasets;

	for (auto dataset : datasets) {
		std::vector<unsigned> valid_selections;

		for (int selection = 0; selection < 6; selection++) {
			switch (selection) {
			case 0:
				std::cout << "hanwen_find_equality_correlation" << std::endl;
				break;
			case 1:
				std::cout << "hanwen_find_dict1to1_correlation" << std::endl;
				//				hanwen_find_dict1to1_correlation(0, 1);
				break;
			case 2:
				std::cout << "hanwen_find_dict1toN_correlation" << std::endl;
				//				hanwen_find_dict1toN_correlation(0, 1);
				break;
			case 3:
				std::cout << "hanwen_find_numerical_correlation" << std::endl;
				//				hanwen_find_numerical_correlation(0, 1);
				break;
			case 4:
				std::cout << "hanwen_find_dfor_correlation" << std::endl;
				//				hanwen_find_dfor_correlation(0, 1);
				break;
			case 5:
				std::cout << "hanwen_find_dictShare_correlation" << std::endl;
				//				hanwen_find_dictShare_correlation(0, 1);
				break;
			}

			//			// compress C3
			//			auto relation_range      = compressor.get_btrblocks_relation(dataset); // 返回的是 表 和 块们
			//			auto c3_compressed_sizes = compressor.compress_table_c3(relation_range.first, relation_range.second, dataset, rg_log_info, bb_info, selection);
			auto                                                       relation_range = compressor.get_btrblocks_relation(dataset);
			std::vector<std::shared_ptr<c3::C3LoggingInfo>>            rg_log_info(relation_range.second.size());
			std::vector<std::vector<std::shared_ptr<c3::ColumnStats>>> bb_info(relation_range.second.size());
			auto c3_compressed_sizes = compressor.hanwen_compress_and_verify_c3(compressor.c3_log_stream, relation_range.first, relation_range.second, dataset, bb_info, rg_log_info);

			// log
			size_t total_uncompressed  = 0;
			size_t total_c3_compressed = 0;
			size_t total_bb_compressed = 0;

			size_t total1_uncompressed  = 0;
			size_t total1_c3_compressed = 0;
			size_t total1_bb_compressed = 0;

			size_t total2_uncompressed  = 0;
			size_t total2_c3_compressed = 0;
			size_t total2_bb_compressed = 0;

			for (size_t row_group = 0; row_group < c3_compressed_sizes.size(); row_group++) {
				for (size_t col = 0; col < relation_range.first.columns.size(); col++) {
					total_uncompressed += bb_info[row_group][col]->uncompressed_size;
					total_c3_compressed += c3_compressed_sizes[row_group][col];
					total_bb_compressed += bb_info[row_group][col]->get_BB_compressed_size();
					if (col == 0) {
						total1_uncompressed += bb_info[row_group][col]->uncompressed_size;
						total1_c3_compressed += c3_compressed_sizes[row_group][col];
						total1_bb_compressed += bb_info[row_group][col]->get_BB_compressed_size();
					} else if (col == 1) {
						total2_uncompressed += bb_info[row_group][col]->uncompressed_size;
						total2_c3_compressed += c3_compressed_sizes[row_group][col];
						total2_bb_compressed += bb_info[row_group][col]->get_BB_compressed_size();
					}
				}
			}
			std::cout << "============ Results ============" << std::endl;
			std::cout << "total_uncompressed: " << total_uncompressed << std::endl;
			std::cout << "total_bb_compressed: " << total_bb_compressed << std::endl;
			std::cout << "total_c3_compressed: " << total_c3_compressed << std::endl;

			double compression_ratio_improvement = 1.0 * total_bb_compressed / total_c3_compressed;
			std::cout << dataset.dataset_name << " C3 compression ratio improvement: " << compression_ratio_improvement << std::endl;
			if (compression_ratio_improvement > 1) {
				valid_selections.push_back(selection);
				std::cout << "============ Hanwen ============" << std::endl;
				double savingrate = 1 - (double)total_c3_compressed / total_bb_compressed;
				std::cout << "================> savingrate: " << savingrate << std::endl;
				std::cout << "total1_uncompressed: " << total1_uncompressed << std::endl;
				std::cout << "total1_bb_compressed: " << total1_bb_compressed << std::endl;
				std::cout << "total1_c3_compressed: " << total1_c3_compressed << std::endl;
				double savingrate1 = 1 - (double)total1_c3_compressed / total1_bb_compressed;
				std::cout << "================> savingrate1: " << savingrate1 << std::endl;

				std::cout << "\ntotal2_uncompressed: " << total2_uncompressed << std::endl;
				std::cout << "total2_bb_compressed: " << total2_bb_compressed << std::endl;
				std::cout << "total2_c3_compressed: " << total2_c3_compressed << std::endl;
				double savingrate2 = 1 - (double)total2_c3_compressed / total2_bb_compressed;
				std::cout << "================> savingrate2: " << savingrate2 << std::endl;
			}
			///

			std::cout << "\n\n\n\n\n\n\n\n\n";
		}

		std::cout << "Valid Selection: \n";
		for (auto selection : valid_selections) {
			std::cout << selection << " ";
		}
	}

	return 0;
}