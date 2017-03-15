const DocStore = require('../../providers/storage/mongodocstore');
const docStore = new DocStore('mongodb://localhost/foo');
docStore.connect().then(() => {
  return docStore.count();
}).then(count => {
  console.log(count);
});