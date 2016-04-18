Package.describe({
    name: 'jalik:ufs-template-helpers',
    version: '0.6.0',
    author: 'karl.stein.pro@gmail.com',
    summary: 'Template helpers package for UploadFS',
    homepage: 'https://github.com/jalik/jalik-ufs',
    git: 'https://github.com/jalik/jalik-ufs.git',
    documentation: 'README.md'
});

Package.onUse(function (api) {
    api.versionsFrom('1.1.0.2');
    api.use('check');
    api.use('matb33:collection-hooks@0.7.13');
    api.use('reactive-var', 'client');
    api.use('templating', 'client');
    api.use('underscore');
    api.use('jalik:ufs@0.6.0');

    api.addFiles('ufs-helpers.js', 'client');
});
