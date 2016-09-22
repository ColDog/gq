process.stdin.resume();
process.stdin.setEncoding('utf8');

const graph = require('./index');

const g = new graph.Graph();

process.stdout.write("> ");

process.stdin.on('data', function (arg) {
    if (arg.trim() === '') {
        process.stdout.write("> ");
        return
    }
    
    let first = arg.split('.')[0];
    
    if (first === 'graph') {
        try {
            Function('graph', 'return ' + arg)(g);
            process.stdout.write("> ");
        } catch (e) {
            console.warn('error processing input:', e)
        }
    } else if (first === 'g') {
        const ctx = new graph.Traversal();
        try {
            Function('g', 'return ' + arg)(ctx);
            g.traverse(ctx, function (err, res) {
                console.log(res);
                process.stdout.write("> ");
            });
        } catch (e) {
            console.warn('error processing input:', e)
        }
    }
});
