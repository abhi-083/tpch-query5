#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <future>
#include <algorithm>
#include <vector>
#include <iomanip>
#include <unordered_map>
#include <unordered_set>

void load_table_data(const std::string &filepath,
                     const std::vector<std::string> &columns,
                     std::vector<std::map<std::string, std::string>> &table_data);

// Function to parse command line arguments
bool parseArgs(int argc, char *argv[], std::string &r_name, std::string &start_date, std::string &end_date, int &num_threads, std::string &table_path, std::string &result_path)
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " --r_name <region_name> --start_date <YYYY-MM-DD> --end_date <YYYY-MM-DD> --threads <num_threads> --table_path <path_to_tables> --result_path <path_to_results>" << std::endl;
        return false;
    }

    auto hasValue = [&, argc](int i)
    {
        return ((i + 1 < argc) && argv[i + 1][0] != '-') ? true : false;
    };

    for (int i{1}; i < argc; ++i)
    {
        std::string arg{argv[i]};
        if (arg == "--r_name" && hasValue(i))
        {
            r_name = argv[++i];
        }
        else if (arg == "--start_date" && hasValue(i))
        {
            start_date = argv[++i];
        }
        else if (arg == "--end_date" && hasValue(i))
        {
            end_date = argv[++i];
        }
        else if (arg == "--threads" && hasValue(i))
        {
            try
            {
                num_threads = std::stoi(argv[++i]);
            }
            catch (const std::exception &e)
            {
                std::cerr << "Invalid value for --threads: " << e.what() << "\n";
                return false;
            }
        }
        else if (arg == "--table_path" && hasValue(i))
        {
            table_path = argv[++i];
        }
        else if (arg == "--result_path" && hasValue(i))
        {
            result_path = argv[++i];
        }
        else
        {
            std::cerr << "Unknown argument: " << arg << std::endl;
            return false;
        }
    }

    if (r_name.empty() || start_date.empty() || end_date.empty() ||
        table_path.empty() || result_path.empty() || num_threads <= 0)
    {
        std::cerr << "Missing required arguments\n";
        return false;
    }

    return true;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string &table_path, std::vector<std::map<std::string, std::string>> &customer_data, std::vector<std::map<std::string, std::string>> &orders_data, std::vector<std::map<std::string, std::string>> &lineitem_data, std::vector<std::map<std::string, std::string>> &supplier_data, std::vector<std::map<std::string, std::string>> &nation_data, std::vector<std::map<std::string, std::string>> &region_data)
{
    const std::vector<std::string> CUSTOMER_COLUMNS = {
        "c_custkey", "c_name", "c_address", "c_nationkey",
        "c_phone", "c_acctbal", "c_mktsegment", "c_comment"};

    const std::vector<std::string> ORDERS_COLUMNS = {
        "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
        "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority",
        "o_comment"};

    const std::vector<std::string> LINEITEM_COLUMNS = {
        "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax",
        "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
        "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};

    const std::vector<std::string> SUPPLIER_COLUMNS = {
        "s_suppkey", "s_name", "s_address", "s_nationkey",
        "s_phone", "s_acctbal", "s_comment"};

    const std::vector<std::string> NATION_COLUMNS = {
        "n_nationkey", "n_name", "n_regionkey", "n_comment"};

    const std::vector<std::string> REGION_COLUMNS = {
        "r_regionkey", "r_name", "r_comment"};

    std::thread t1(load_table_data, table_path + "customer.tbl", CUSTOMER_COLUMNS, std::ref(customer_data));
    std::thread t2(load_table_data, table_path + "orders.tbl", ORDERS_COLUMNS, std::ref(orders_data));
    std::thread t3(load_table_data, table_path + "lineitem.tbl", LINEITEM_COLUMNS, std::ref(lineitem_data));
    std::thread t4(load_table_data, table_path + "supplier.tbl", SUPPLIER_COLUMNS, std::ref(supplier_data));
    std::thread t5(load_table_data, table_path + "nation.tbl", NATION_COLUMNS, std::ref(nation_data));
    std::thread t6(load_table_data, table_path + "region.tbl", REGION_COLUMNS, std::ref(region_data));

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();

    std::cout << "------------------------------------------\n";
    std::cout << "Loaded " << customer_data.size() << " rows into Customer.\n";
    std::cout << "Loaded " << orders_data.size() << " rows into Orders.\n";
    std::cout << "Loaded " << lineitem_data.size() << " rows into Lineitem.\n";
    std::cout << "Loaded " << supplier_data.size() << " rows into Supplier.\n";
    std::cout << "Loaded " << nation_data.size() << " rows into Nation.\n";
    std::cout << "Loaded " << region_data.size() << " rows into Region.\n";
    std::cout << "------------------------------------------\n";

    return true;
}

template <typename ResultType, typename FilterFunc>
ResultType parallelFilter(const std::vector<std::map<std::string, std::string>> &table_data, FilterFunc filter_func, int num_threads)
{
    ResultType final_result;
    size_t dataSize = table_data.size();
    size_t chunkSize = dataSize / num_threads;
    std::vector<std::future<ResultType>> futures;
    futures.reserve(num_threads);

    for (size_t i = 0; i < num_threads; ++i)
    {
        size_t start = i * chunkSize;
        size_t end = (i == num_threads - 1) ? dataSize : start + chunkSize;

        futures.emplace_back(std::async(std::launch::async, [start, end, &table_data, &filter_func]()
                                        {
                                            ResultType local_result;
                                            for (size_t j = start; j < end; ++j) {
                                                filter_func(table_data[j], local_result);
                                            }
                                            return local_result; }));
    }

    for (auto &future : futures)
    {
        auto local_result = future.get();
        for (const auto &[val1, val2] : local_result)
        {
            final_result[val1] += val2;
        }
    }

    return final_result;
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string &r_name, const std::string &start_date, const std::string &end_date, int num_threads, const std::vector<std::map<std::string, std::string>> &customer_data, const std::vector<std::map<std::string, std::string>> &orders_data, const std::vector<std::map<std::string, std::string>> &lineitem_data, const std::vector<std::map<std::string, std::string>> &supplier_data, const std::vector<std::map<std::string, std::string>> &nation_data, const std::vector<std::map<std::string, std::string>> &region_data, std::map<std::string, double> &results)
{
    std::string region_key;
    for (auto &row : region_data)
    {
        if (row.at("r_name") == r_name)
        {
            region_key = row.at("r_regionkey");
            break;
        }
    }

    auto filtered_nations = parallelFilter<std::unordered_map<std::string, std::string>>(
        nation_data,
        [&](const auto &row, auto &result)
        {
            if (row.at("n_regionkey") == region_key)
            {
                result[row.at("n_nationkey")] = row.at("n_name");
            }
        },
        num_threads);

    std::unordered_set<std::string> valid_nation_keys;
    valid_nation_keys.reserve(filtered_nations.size());
    for (const auto &[nation_key, nation_name] : filtered_nations)
    {
        valid_nation_keys.insert(nation_key);
    }

    auto filtered_suppliers = parallelFilter<std::unordered_map<std::string, std::string>>(
        supplier_data,
        [&](const auto &row, auto &result)
        {
            if (valid_nation_keys.count(row.at("s_nationkey")))
            {
                result[row.at("s_suppkey")] = row.at("s_nationkey");
            }
        },
        num_threads);

    std::unordered_set<std::string> valid_supplier_nation_keys;
    valid_supplier_nation_keys.reserve(filtered_suppliers.size());

    for (const auto &[supplier_key, nation_key] : filtered_suppliers)
    {
        valid_supplier_nation_keys.insert(nation_key);
    }

    auto filtered_customers = parallelFilter<std::unordered_map<std::string, std::string>>(customer_data, [&](const auto &row, auto &result)
                                                                                           {
        if (valid_supplier_nation_keys.count(row.at("c_nationkey"))) {
            result[row.at("c_custkey")] = row.at("c_nationkey");
        } }, num_threads);

    auto filtered_orders = parallelFilter<std::unordered_map<std::string, std::string>>(orders_data, [&](const auto &row, auto &result)
                                                                                        {
        const auto& date = row.at("o_orderdate");
        if (date >= start_date && date < end_date && 
            filtered_customers.count(row.at("o_custkey"))) {
                result[row.at("o_orderkey")] = row.at("o_custkey");
        } }, num_threads);

    results = parallelFilter<std::map<std::string, double>>(
        lineitem_data,
        [&](const auto &row, auto &result)
        {
            if (filtered_suppliers.count(row.at("l_suppkey")) &&
                filtered_orders.count(row.at("l_orderkey")))
            {
                auto nation_name = filtered_nations.at(
                    filtered_customers.at(filtered_orders.at(row.at("l_orderkey"))));
                result[nation_name] += std::stod(row.at("l_extendedprice")) *
                                       (1 - std::stod(row.at("l_discount")));
            }
        },
        num_threads);

    return true;
}
bool outputResults(const std::string &result_path, const std::map<std::string, double> &results)
{
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());

    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const std::pair<std::string, double> &a, const std::pair<std::string, double> &b)
              {
                  return a.second > b.second;
              });

    auto output_file = result_path + "query5_results" + std::to_string(std::time(nullptr)) + ".tbl";
    std::ofstream ofs(output_file);

    if (!ofs.is_open())
    {
        std::cerr << "Error: Could not open output file " << output_file << "\n";
        return false;
    }

    ofs << "n_name|revenue\n";

    ofs << std::fixed;
    ofs.precision(2);
    for (const auto &row : sorted_results)
    {
        ofs << row.first << "|" << row.second << "\n";
    }

    ofs.close();

    std::cout << "------------------------------------------\n";
    std::cout << "Query 5 Results (sorted by revenue desc):\n";
    std::cout << "------------------------------------------\n";
    std::cout << std::fixed;
    std::cout.precision(2);
    for (const auto &row : sorted_results)
    {
        std::cout << row.first << " | " << row.second << "\n";
    }
    std::cout << "------------------------------------------\n";

    std::cout << "Results written to: " << output_file << "\n";
    return true;
}

void load_table_data(const std::string &filepath,
                     const std::vector<std::string> &columns,
                     std::vector<std::map<std::string, std::string>> &table_data)
{

    std::ifstream file(filepath);
    if (!file.is_open())
    {
        std::cerr << "Error: Could not open " << filepath << "\n";
        return;
    }

    std::string line;
    while (std::getline(file, line))
    {
        if (line.empty())
        {
            continue;
        }

        std::stringstream ss(line);
        std::string token;
        std::map<std::string, std::string> row_map;

        size_t col_index = 0;
        while (std::getline(ss, token, '|'))
        {
            if (col_index >= columns.size())
            {
                break;
            }

            row_map[columns[col_index]] = token;
            col_index++;
        }

        table_data.push_back(row_map);
    }

    file.close();
}