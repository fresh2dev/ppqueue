# https://realpython.com/pypi-publish-python-package

# pip install setuptools wheel
# pip install twine

Remove-Item 'build', 'dist', 'ezpq.egg-info' -Recurse -Force -ea 0

pandoc --from=markdown --to=commonmark -o README.md README.md
pandoc --from=commonmark --to=rst --output=README.rst README.md

# fix  'Warning: "raw" directive disabled.'
$rst = Get-Content 'README.rst' | Where-Object { $_ -ne '.. raw:: html' }
$rst | Out-File 'README.rst'

python setup.py sdist bdist_wheel

twine check dist/*

twine upload --repository-url https://test.pypi.org/legacy/ dist/*

python3 -m pip install --index-url https://test.pypi.org/simple/ ezpq

# twine upload dist/*


