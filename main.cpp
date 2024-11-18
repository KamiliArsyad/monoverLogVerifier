#include <iostream>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>

struct Operation {
    long long int sesId;
    std::string opType;
    int objectId;
};

class ElleOp
{
public:
    explicit ElleOp(int objId, long long int transactionId) : objId(objId), transactionId(transactionId) {}
    virtual ~ElleOp() = default;

    virtual std::string toString() = 0;

    virtual void setToInvoke() = 0;

    virtual void setToOk() = 0;

    long long int transactionId;
protected:
    int objId;
};

class ReadOp final : public ElleOp
{
public:
    ReadOp(int objVersion, int objId, long long int transactionId) : ElleOp(objId, transactionId), objVersion(objVersion) {}

    std::string toString() override
    {
        std::stringstream ss;
        ss << "[:r " << objId;

        if (objVersion == -1 || this->isInvoke)
        {
            ss << " nil]";
            return ss.str();
        }

        ss << " [";
        for (int i = 0; i < objVersion; i++)
        {
            ss << i << " ";
        }
        ss << objVersion << "]]";

        return ss.str();
    }

    void setToInvoke() override
    {
        this->isInvoke = true;
    }

    void setToOk() override
    {
        this->isInvoke = false;
    }

private:
    int objVersion;

    // For invoke read operation in EDN, the read "values" should be "nil"
    bool isInvoke = false;
};

class WriteOp final : public ElleOp
{
public:
    WriteOp(int objVersion, int objId, long long int transactionId) : ElleOp(objId, transactionId), objVersion(objVersion) {}

    std::string toString() override
    {
        std::stringstream ss;
        ss << "[:append " << objId << " " << objVersion << "]";

        return ss.str();
    }

    void setToInvoke() override
    {
        return;
    }

    void setToOk() override
    {
        return;
    }

private:
    int objVersion;
};

std::vector<Operation> parseOpLog(const std::string &filename) {
    std::vector<Operation> transactions;
    std::ifstream file(filename);
    std::string line;

    // Define operation keywords and corresponding attributes
    std::unordered_map<std::string, std::pair<std::string, bool>> operations = {
        {"BEGIN", {"BEGIN", false}},
        {"WRITE", {"WRITE", true}},
        {"READ", {"READ", true}},
        {"COMMIT", {"COMMIT", false}}
    };

    while (std::getline(file, line)) {
        // Remove the $$ delimiters
        size_t start = line.find("$_$_$");
        size_t end = line.rfind("$_$_$");
        if (start == std::string::npos || end == std::string::npos || start == end) {
            continue;
        }
        std::string content = line.substr(start + 2, end - start - 2);

        std::istringstream iss(content);
        Operation transaction;
        std::string temp;

        // Extract operation type
        iss >> temp >> transaction.opType;

        if (operations.find(transaction.opType) != operations.end()) {
            transaction.sesId = -1;
            transaction.objectId = -1;
            if (transaction.opType == "BEGIN" || transaction.opType == "COMMIT") {
                iss >> temp >> transaction.sesId;
            } else if (transaction.opType == "WRITE" || transaction.opType == "READ") {
                iss >> temp >> transaction.sesId >> temp >> transaction.objectId;
            }

            transactions.push_back(transaction);
        }
    }

    return transactions;
}

// Given a transaction mapping, print out the elle-compatible log to the standard output.
void writeToFile(const std::string &filename, const std::map<long long int, std::vector<std::unique_ptr<ElleOp>>> &transactions, std::map<long long int, std::pair<int, int>> &timing);

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <input_filename> <output_file>" << std::endl;
        return 1;
    }

    const std::string filename = argv[1];
    const std::string outputFile = argv[2];
    const std::vector<Operation> operations = parseOpLog(filename);
    std::map<long long int, long long int> sessionToTxMap;
    std::map<int, int> objVersionMap;
    std::map<long long int, std::vector<std::unique_ptr<ElleOp>>> transactions;
    std::map<long long int, std::pair<int, int>> transactionTime;
    int currTime = 0;

    for (const auto &operation : operations) {
        if (!sessionToTxMap.contains(operation.sesId))
        {
            sessionToTxMap[operation.sesId] = operation.sesId;
        }

        const long long int tx = sessionToTxMap[operation.sesId];

        if (operation.opType == "BEGIN")
        {
            transactionTime[tx] = {currTime, -1};
            currTime++;
        }
        else if (operation.opType == "COMMIT")
        {
            sessionToTxMap[operation.sesId] += operation.sesId;
            transactionTime[tx].second = currTime;
            currTime++;
        } else if (operation.opType == "WRITE")
        {
            if (!objVersionMap.contains(operation.objectId))
            {
                // Initializes the object version
                objVersionMap[operation.objectId] = -1;
            }

            objVersionMap[operation.objectId] += 1;
            transactions[tx].push_back(std::make_unique<WriteOp>(objVersionMap[operation.objectId], operation.objectId, tx));
            transactionTime[tx].second = currTime; /* temporarily set the transaction commit time to the last operation */
        } else if (operation.opType == "READ")
        {
            if (!objVersionMap.contains(operation.objectId))
            {
                // Initializes the object version
                objVersionMap[operation.objectId] = -1;
            }
            transactions[tx].push_back(std::make_unique<ReadOp>(objVersionMap[operation.objectId], operation.objectId, tx));
            transactionTime[tx].second = currTime; /* temporarily set the transaction commit time to the last operation */
        }
    }

    writeToFile(outputFile, transactions, transactionTime);

    return 0;
}


void writeToFile(const std::string &filename, const std::map<long long int,
    std::vector<std::unique_ptr<ElleOp>>> &transactions,
    std::map<long long int, std::pair<int, int>> &timing)
{
    std::ofstream file(filename);
    if (!file.is_open())
    {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }

    int index = 0;
    std::map<long long int, std::string> elleOut;
    for (const auto &tx : transactions)
    {
        // Write invoke type
        std::stringstream ss;
        ss << "{:index " << index++ << " :type :invoke, :value [";
        for (const auto &op : tx.second)
        {
            op->setToInvoke();
            ss << op->toString() << " ";
        }

        ss << "], "
            << ":process " << tx.first << ", "
            << ":time " << timing[tx.first].first << "}"
            << std::endl;

        elleOut[timing[tx.first].first] = ss.str();
        ss = std::stringstream();

        // Write ok type

        ss << "{:index " << index++ << " :type :ok, :value [";
        for (const auto &op : tx.second)
        {
            op->setToOk();
            ss << op->toString() << " ";
        }

        ss << "], "
            << ":process " << tx.first << ", "
            << ":time " << timing[tx.first].second << "}"
            << std::endl;

        elleOut[timing[tx.first].second] = ss.str();
    }

    for (const auto &output : elleOut)
    {
        file << output.second;
    }
}