// mongoDBにログインして手動で実行
// コレクション名は適宜変更

// 重複しているものをカウントしIDを特定
db.suumo.aggregate( { $group : {_id :"$building_name", dups:{$push:"$_id"}, count : {$sum : 1}}}, {$match : {count: { $gt:1}}})



//　重複しているIDで最初のものを残し、2番目から削除する
db.suumo.aggregate( { $group : {_id :"$building_name", dups:{$push:"$_id"}, count : {$sum : 1}}}, {$match : {count: { $gt:1}}}).forEach(function(doc){ doc.dups.shift(); db.suumo.remove({_id : {$in : doc.dups}}); })
