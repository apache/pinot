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
        files: [
          {
            expand: true,
            cwd: 'src/main/resources/assets/javascript/',
            src: ['**/*.js'],
            dest: 'target/classes/assets/javascript/'
          }
        ]
      }
    },

    watch: {
      scripts: {
        files: 'src/main/resources/assets/javascript/**/*.js',
        tasks: ['default']
      }
    }
  });

  grunt.registerTask('default', ['babel']);
};
