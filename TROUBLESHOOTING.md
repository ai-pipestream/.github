# GitHub Pages Troubleshooting Guide

This guide helps resolve common issues with GitHub Pages deployment for the Pipestream AI homepage.

## Issue: Empty nginx page or 404 error

### Solution

The most common cause is incorrect GitHub Pages source configuration. Follow these steps:

1. **Go to Repository Settings**
   - Navigate to https://github.com/ai-pipestream/.github/settings/pages

2. **Configure the Source**
   - Under **"Build and deployment"** section
   - For **"Source"**, select **"GitHub Actions"** (NOT "Deploy from a branch")
   - This allows the workflow to deploy the site

3. **Verify Custom Domain** (if using pipestream.ai)
   - The **"Custom domain"** field should show: `pipestream.ai`
   - If not, enter it and click "Save"
   - GitHub will verify DNS and create a commit with CNAME file (already exists in this repo)

4. **Enable "Enforce HTTPS"**
   - Check the box for **"Enforce HTTPS"**
   - This may take a few minutes to become available after DNS verification

### After Configuration

1. The workflow will automatically trigger on the next push to `main`
2. Check the Actions tab: https://github.com/ai-pipestream/.github/actions
3. Wait for the "Deploy GitHub Pages" workflow to complete (usually 1-2 minutes)
4. Visit https://pipestream.ai to see your site

## Issue: Workflow runs but site doesn't update

### Possible Causes

1. **Wrong Source Selected**
   - Verify "GitHub Actions" is selected as source (see above)

2. **Deployment Failed**
   - Check workflow logs: https://github.com/ai-pipestream/.github/actions
   - Look for error messages in the deploy step

3. **Caching**
   - Try hard refresh: Ctrl+F5 (Windows/Linux) or Cmd+Shift+R (Mac)
   - Clear browser cache
   - Try incognito/private browsing mode

## Issue: DNS not resolving

### DNS Configuration Required

For `pipestream.ai` to work, you need to configure DNS at your domain registrar:

1. **A Records** (for apex domain):
   ```
   185.199.108.153
   185.199.109.153
   185.199.110.153
   185.199.111.153
   ```

2. **CNAME Record** (if using subdomain like www or docs):
   ```
   CNAME: ai-pipestream.github.io
   ```

See [SETUP.md](SETUP.md) for detailed DNS configuration instructions.

## Issue: SSL/HTTPS certificate errors

### Solution

GitHub automatically provisions SSL certificates via Let's Encrypt, but this requires:

1. DNS must be properly configured and propagated
2. "Enforce HTTPS" must be enabled in repository settings
3. Wait up to 24 hours for certificate provisioning

If still having issues after 24 hours:
1. Uncheck "Enforce HTTPS" in settings
2. Remove and re-add the custom domain
3. Wait for DNS verification
4. Re-enable "Enforce HTTPS"

## Checking Deployment Status

### View Workflow Runs
https://github.com/ai-pipestream/.github/actions/workflows/pages.yml

### View Live Site
- Custom domain: https://pipestream.ai
- GitHub URL: https://ai-pipestream.github.io/.github/

### Verify DNS
```bash
# Check A records
dig pipestream.ai A

# Check CNAME records (if using subdomain)
dig www.pipestream.ai CNAME

# Verify GitHub Pages is serving the site
curl -I https://ai-pipestream.github.io/.github/
```

## Getting Help

If you've followed all steps and still have issues:

1. Check GitHub Status: https://www.githubstatus.com/
2. Review workflow logs for specific error messages
3. Open an issue in this repository with:
   - What you've tried
   - Error messages
   - Workflow run links
   - DNS configuration (if applicable)
