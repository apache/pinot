// "use strict";

// module.exports = (grunt) => {
//   require('load-grunt-tasks')(grunt);

//   // Project configuration
//   grunt.initConfig({
//     babel: {
//       options: {
//         sourceMap: true,
//         presets: ['es2015']
//       },
//       dist: {
//         // files: {
//         //   'dist/app.js': './src/main/resources/assets/javascript/HashService.js'
//         // }
//         files: [
//           {
//             expand: true,
//             cwd: 'src/main/resources/assets/javascript/',
//             src: ['**/*.js'],
//             dest: 'target/classes/assets/javascript/'
//           }
//         ]
//       }
//     }
//   });

//   grunt.registerTask('default', ['babel']);
// };


"use strict";

module.exports = (grunt) => {
  require('load-grunt-tasks')(grunt);

  // Project configuration
  grunt.initConfig({
    babel: {
      options: {
        sourceMap: true,
        presets: ['es2015']
      },
      dist: {
        // files : {
        //   "target/classes/assets/javascript/app.js": "src/main/resources/assets/javascript/*.js"
        // }
        files: [
          {
            expand: true,
            cwd: 'src/main/resources/assets/javascript/',
            src: ['**/*.js'],
            dest: 'src/main/resources/assets/javascript-compiled/',
            ext: "-compiled.js"
          }
        ]
      }
    }
  });

  grunt.registerTask('default', ['babel']);
};
