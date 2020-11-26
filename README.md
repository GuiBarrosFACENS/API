/**
 * Script simples para importação de dados de cotação da Bovespa
 *
 * Requisitos:
 *   - Node.js instalado
 *   - instalar as dependencias: npm install extract-zip moment request
 *
 * Exemplo de uso: node script_simples_importaca_cota_hist_bovespa.js 05022019
 *ENDPOINT AQUI <--
 */

const fs = require('fs'),
  path = require('path'),
  extract = require('extract-zip'),
  moment = require('moment'),
  request = require('request');

const bovespaHD = {
  tmpFolder: path.resolve(process.cwd()),

  /**
   * Função para retornar os dados de cotação de 1 dia
   *
   * @param  {Object}   date  moment.js date
   * @param  {Function} done  callback
   */
  getDailyData(date, done) {
    let day = date.format('DDMMYYYY');

    let zipName = 'COTAHIST_D'+day+'.ZIP';
    let name = 'COTAHIST_D'+day+'.TXT';

    let externalFileName = 'http://bvmf.bmfbovespa.com.br/InstDados/SerHist/'+zipName;
    let zipDest = path.join(this.tmpFolder, zipName);
    let contentsDest = path.join(this.tmpFolder, 'COTAHIST_D'+day);
    let contentFile = path.join(this.tmpFolder, 'COTAHIST_D'+day, name);

    this.downloadFile(externalFileName, zipDest, (err)=> {
      if (err) return done(err);

      this.extractFileContents(zipDest, contentsDest, (err)=> {
        if (err) return done(err);

        this.parseFileData(contentFile, done);
      });
    });
  },

  downloadFile(url, dest, done) {
    const r = request({ uri: url });

    r.on('response', (resp)=> {
      if (resp.statusCode == '404') {
        return done('List not avaible for selected date');
      } else if (resp.statusCode != '200') {
        return done(JSON.stringify(resp));
      }

      r.pipe(fs.createWriteStream(dest))
      .on('close', ()=> {
        done();
      });
    });
  },
  extractFileContents(source, target, cb) {
    extract(source, { dir: target })
    .then((r)=> {
      cb(null, r);
    })
    .catch(cb);
  },  
  parseFileData(file, done) {
    fs.readFile(file, 'ascii', (err, contents)=> {
      if (err) return done(err);

      let lines = contents.split('\r\n');

      const parsedData = [];

      let length = lines.length-2; // ignore last 2 lines

      for (let i = 1; i < length; i++) {
        let line = lines[i];

        if (line.length === 245) {
          parsedData.push(this.parseLine(line));
        } else {
          console.log('Unknow line size>', i, line.length, line);
        }
      }

      done(null, parsedData);
    });
  },
  parseLine(line) {
    let historicRow = {};
    // TIPREG - TIPO DE REGISTRO
    historicRow.TIPREG = line.substring(0, 2);
    // DATA DO PREGÃO
    historicRow.date = line.substring(2, 10);
    // CODBDI - CÓDIGO BDI
    historicRow.CODBDI = line.substring(10, 12);
    // CODNEG - CÓDIGO DE NEGOCIAÇÃO DO PAPEL
    historicRow.CODNEG = (line.substring(12, 24)).trim();
    // symbol:
    historicRow.symbol = ('BVMF:'+historicRow.CODNEG).trim();
    // date/value identifier:
    historicRow.identifier = historicRow.symbol+'::'+historicRow.date;

    historicRow.date = moment(historicRow.date, 'YYYYMMDD').format();
    // TPMERC - TIPO DE MERCADO
    historicRow.TPMERC = line.substring(24, 27);
    // NOMRES - NOME RESUMIDO DA EMPRESA EMISSORA DO PAPEL
    historicRow.NOMRES = (line.substring(27, 39)).trim();
    // ESPECI - ESPECIFICAÇÃO DO PAPEL
    historicRow.ESPECI = line.substring(39, 49);
    // PRAZOT - PRAZO EM DIAS DO MERCADO A TERMO
    historicRow.PRAZOT = line.substring(49, 52);
    // MODREF - MOEDA DE REFERÊNCIA
    historicRow.MODREF = line.substring(52, 56);
    // PREABE - PREÇO DE ABERTURA DO PAPEL- MERCADO NO PREGÃO
    historicRow.PREABE = addDot( line.substring(56, 69) );
    // PREMAX - PREÇO MÁXIMO DO PAPEL- MERCADO NO PREGÃO
    historicRow.PREMAX = addDot( line.substring(69, 82) );
    // PREMIN - PREÇO MÍNIMO DO PAPEL- MERCADO NO PREGÃO
    historicRow.PREMIN = addDot( line.substring(82, 95) );
    // PREMED - PREÇO MÉDIO DO PAPEL- MERCADO NO PREGÃO
    historicRow.PREMED = addDot( line.substring(95, 108) );
    // PREULT - PREÇO DO ÚLTIMO NEGÓCIO DO PAPEL-MERCADO NO PREGÃO
    historicRow.PREULT = addDot( line.substring(108, 121) );
    // PREOFC - PREÇO DA MELHOR OFERTA DE COMPRA DO PAPEL- MERCADO
    historicRow.PREOFC = addDot( line.substring(121, 134) );
    // PREOFV - PREÇO DA MELHOR OFERTA DE VENDA DO PAPEL- MERCADO
    historicRow.PREOFV = addDot( line.substring(134, 147) );
    // TOTNEG - NEG. - NÚMERO DE NEGÓCIOS EFETUADOS COM O PAPEL- MERCADO NO PREGÃO

    historicRow.TOTNEG = line.substring(147, 152);
    // QUATOT - QUANTIDADE TOTAL DE TÍTULOS NEGOCIADOS NESTE PAPEL- MERCADO
    historicRow.QUATOT = line.substring(147, 170);

    // VOLTOT - VOLUME TOTAL DE TÍTULOS NEGOCIADOS NESTE PAPEL- MERCADO
    historicRow.VOLTOT = addDot( line.substring(170, 188) );
    // PREEXE - PREÇO DE EXERCÍCIO PARA O MERCADO DE OPÇÕES OU VALOR DO CONTRATO PARA O MERCADO DE TERMO SECUNDÁRIO
    historicRow.PREEXE = addDot( line.substring(188, 201) );

    // INDOPC - INDICADOR DE CORREÇÃO DE PREÇOS DE EXERCÍCIOS OU VALORES DE CONTRATO PARA OS MERCADOS DE OPÇÕES OU TERMO SECUNDÁRIO
    historicRow.INDOPC = line.substring(201, 202);
    // DATVEN - DATA DO VENCIMENTO PARA OS MERCADOS DE OPÇÕES OU TERMO SECUNDÁRIO
    historicRow.DATVEN = line.substring(202, 210);
    // FATCOT - FATOR DE COTAÇÃO DO PAPEL
    historicRow.FATCOT = line.substring(210, 217);
    // PTOEXE - PREÇO DE EXERCÍCIO EM PONTOS PARA OPÇÕES REFERENCIADAS EM DÓLAR OU VALOR DE CONTRATO EM PONTOS PARA TERMO SECUNDÁRIO
    historicRow.PTOEXE = addDot( line.substring(217, 230), 6);
    // CODISI - CÓDIGO DO PAPEL NO SISTEMA ISIN OU CÓDIGO INTERNO DO PAPEL
    historicRow.CODISI = line.substring(230, 242);
    // DISMES - NÚMERO DE DISTRIBUIÇÃO DO PAPEL
    historicRow.DISMES = line.substring(242, 245);

    return historicRow;
  }
}

// Helpers:

function addDot(str, right) {
  if (!right) right = 2;

 return str.substring(0,str.length - right)+'.'+str.substring(str.length - right);
}


module.exports = bovespaHD;

// Extra code for allow file execution:
if (require.main === module) {
  console.log('called directly');

  let args = process.argv.slice(2);

  if (args && args.length) {
    let d = moment(args[0], 'DDMMYYYY');
    if (d.isValid()) {

      bovespaHD.getDailyData(d, (err, d)=> {
        if (err) console.warn('Error on result:', err);
        console.log('result>', d);
        process.exit();
      });

    }
  }
}
