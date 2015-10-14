// Gulp and plugins
var gulp = require('gulp'),
  umd    = require('gulp-umd'),
  rimraf = require('gulp-rimraf'),
  concat = require('gulp-concat'),
  jshint = require('gulp-jshint'),
  connect = require('gulp-connect'),
  testem = require('gulp-testem'),
  replace = require('gulp-replace'),
  rename = require('gulp-rename'),
  sass = require('gulp-sass');

var pjson = require('./package.json');

var filename = pjson.name.replace(/-/g, '_');

// paths
var src = './src/',
  dist = './dist/',
  jsFiles = src + 'js/**/*.js',
  scssFiles = src + 'scss/**/*.scss';

gulp.task('default', ['jshint', 'clean', 'test', 'build:js', 'build:css']);

gulp.task('clean', function () {
  return gulp.src([dist + '*', 'tmp/*'], {read: false})
    .pipe(rimraf());
});

gulp.task('build:js', function () {
  return gulp.src(jsFiles)
    .pipe(concat({path: filename + '.js'}))
    .pipe(umd(
        {
          dependencies:function() {
            return [{
              name: 'd3',
              amd: 'd3',
              cjs: 'd3',
              global: 'd3',
              param: 'd3'
            },
            {
              name: 'jquery',
              amd: 'jquery',
              cjs: 'jquery',
              global: 'jQuery',
              param: '$'
            },
            {
              name: 'MG',
              amd: 'MG',
              cjs: 'MG',
              global: 'MG',
              param: 'MG'
            }];
          },
          exports: function() {
            return null;
          }
        }
    ))
    .pipe(gulp.dest(dist));
});

// build css files from scss
gulp.task('build:css', function () {
  return gulp.src(scssFiles)
    .pipe(sass())
    .pipe(rename(filename + '.css'))
    .pipe(gulp.dest(dist));
});

// Check source js files with jshint
gulp.task('jshint', function () {
  return gulp.src(jsFiles)
    .pipe(jshint())
    .pipe(jshint.reporter('default'));
});

// Run test suite server (testem')
gulp.task('test', function() {
  return gulp.src([''])
    .pipe(testem({
      configFile: 'testem.json'
    }));
});

// Development server tasks
var roots = ['src', 'dev'],
    watchables = roots.map(function(root) {
        return root + '/**/*';
    });

gulp.task('dev:watch', function() {
  return gulp.watch(watchables, ['jshint', 'build:js', 'dev:setup', 'dev:reload']);
});

gulp.task('dev:reload', function() {
  return gulp.src(watchables).pipe(connect.reload());
});

gulp.task('serve', ['jshint', 'dev:serve', 'dev:watch']);

gulp.task('dev:setup', ['dev:prepareEnv'], function() {
  return gulp.src('tmp/serve/index.html')
    .pipe(replace(/{{PACKAGE_NAME}}/g, filename))
    .pipe(gulp.dest('tmp/serve'));
});

gulp.task('dev:prepareEnv', ['clean', 'build:js', 'build:css'], function() {
  return gulp.src(['dev/**/*.*', 'dist/**/*.*', 'bower_components/**/*.*'])
    .pipe(gulp.dest('tmp/serve'));
});

gulp.task('dev:serve', ['dev:setup'], function() {
    connect.server({
        root: 'tmp/serve',
        port: 4300,
        livereload: true
    });
});
