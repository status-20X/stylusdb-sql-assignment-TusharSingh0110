const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses } = parseQuery(query);

  
    const data = await readCSV(`${table}.csv`);

    
    let filteredData = data;
    if (whereClauses && whereClauses.length > 0) {
       
        filteredData = data.filter(row =>
            whereClauses.every(clause => {
                const { field, operator, value } = clause;

                
                switch (operator) {
                    case '=':
                        return row[field] === value;
                    case '!=':
                        return row[field] !== value;
                    case '<':
                        return row[field] < value;
                    case '<=':
                        return row[field] <= value;
                    case '>':
                        return row[field] > value;
                    case '>=':
                        return row[field] >= value;
                    default:
                        throw new Error(`Unsupported operator: ${operator}`);
                }
            })
        );
    }

  
    return filteredData.map(row => {
        const filteredRow = {};
        fields.forEach(field => {
            filteredRow[field] = row[field];
        });
        return filteredRow;
    });
}

module.exports = executeSELECTQuery;
