const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClause } = parseQuery(query);

   
    const data = await readCSV(`${table}.csv`);
    
  
    let filteredData = data;
    if (whereClause) {
        const [field, value] = whereClause.split('=').map(s => s.trim());
        
        filteredData = data.filter(row => row[field] === value);
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
