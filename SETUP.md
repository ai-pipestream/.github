# GitHub Pages Setup Guide

This repository is configured to serve as the organization homepage for AI Pipestream using GitHub Pages with a custom domain.

## Overview

This setup enables:
- **Organization Homepage**: The `.github` repository serves as the public face of the organization
- **Custom Domain**: Access via pipestream.ai (or docs.pipestream.ai)
- **HTTPS/SSL**: GitHub provides automatic SSL certificates or you can use your own wildcard certificate
- **Automated Deployment**: GitHub Actions automatically deploys changes to the main branch

## Initial Setup Steps

### 1. Enable GitHub Pages

1. Go to the repository settings: `https://github.com/ai-pipestream/.github/settings/pages`
2. Under "Source", select "GitHub Actions" as the deployment method
3. The workflow in `.github/workflows/pages.yml` will handle the deployment

### 2. Configure Custom Domain

#### Option A: Using pipestream.ai (Apex Domain)

1. In your DNS provider, add these records:
   ```
   Type: A
   Name: @
   Value: 185.199.108.153
   
   Type: A
   Name: @
   Value: 185.199.109.153
   
   Type: A
   Name: @
   Value: 185.199.110.153
   
   Type: A
   Name: @
   Value: 185.199.111.153
   ```

2. Also add a CNAME record for the www subdomain:
   ```
   Type: CNAME
   Name: www
   Value: ai-pipestream.github.io
   ```

#### Option B: Using docs.pipestream.ai (Subdomain)

If you prefer to use a subdomain instead, update the `CNAME` file to contain `docs.pipestream.ai` and add this DNS record:

```
Type: CNAME
Name: docs
Value: ai-pipestream.github.io
```

### 3. SSL Certificate Configuration

GitHub Pages provides **automatic HTTPS** using Let's Encrypt certificates. This is the recommended approach as it requires no manual configuration and renews automatically.

#### Automatic SSL (Recommended) ✅

1. After DNS propagation (can take up to 24 hours), go to repository Settings > Pages
2. Check the "Enforce HTTPS" option
3. GitHub will automatically provision and renew SSL certificates

**This is the best option for most use cases** - it's free, automatic, secure, and requires zero maintenance.

#### Using Your Custom Wildcard SSL Certificate

If you have a specific wildcard SSL certificate you want to use, here are your options:

**Important**: GitHub Pages does **not** support uploading custom SSL certificates directly through their UI or API. However, you can still use your certificate through these methods:

##### Option 1: Cloudflare (Recommended for Custom Certs)

Cloudflare offers a free tier that lets you use your custom SSL certificate:

1. **Set up Cloudflare**:
   - Sign up at https://cloudflare.com (free tier available)
   - Add your domain (pipestream.ai)
   - Update your domain's nameservers to Cloudflare's

2. **Configure DNS in Cloudflare**:
   ```
   Type: CNAME
   Name: @ (or www, or docs)
   Target: ai-pipestream.github.io
   Proxy status: Proxied (orange cloud)
   ```

3. **Upload Your Custom Certificate**:
   - Go to SSL/TLS → Custom Certificates
   - Upload your wildcard certificate (.crt) and private key (.key)
   - Cloudflare will use your certificate for connections to your domain

4. **Configure SSL Settings**:
   - SSL/TLS encryption mode: "Full" (Cloudflare → GitHub also encrypted)
   - Always Use HTTPS: On
   - Automatic HTTPS Rewrites: On

**Benefits**: 
- Use your specific certificate
- Additional CDN performance benefits
- DDoS protection
- Analytics
- Free tier available

##### Option 2: Store Certificate as GitHub Secret (For Advanced Workflows)

While you can't use custom certificates directly with GitHub Pages, you can store your certificate as a repository secret for other purposes:

1. Go to Repository Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Add your certificate components:
   - Name: `SSL_CERTIFICATE` (value: certificate .crt content)
   - Name: `SSL_PRIVATE_KEY` (value: private key content)
   - Name: `SSL_CA_BUNDLE` (value: CA bundle if applicable)

**Note**: These secrets can be used in GitHub Actions workflows for deployment to other hosting platforms if needed, but cannot be used with GitHub Pages directly.

##### Option 3: Alternative Hosting with Custom Certificates

If you absolutely need to use your certificate directly (not through Cloudflare), consider:

1. **GitHub Enterprise Cloud**: Supports custom SSL certificates natively
2. **Netlify**: Free tier, supports custom certificates
3. **Vercel**: Free tier, automatic SSL but also supports custom certs in paid plans
4. **AWS S3 + CloudFront**: Full control, use ACM or upload custom certs
5. **Self-hosted**: nginx/Apache with your certificate

For most users, **GitHub's automatic Let's Encrypt certificates or Cloudflare with your custom certificate** will be the best options.

### 4. Verify Setup

After DNS propagation (typically 24-48 hours):

1. Visit your custom domain (e.g., https://pipestream.ai)
2. Verify the page loads correctly
3. Check that HTTPS is working (look for the lock icon in your browser)
4. Test the www subdomain if using an apex domain

## Updating the Homepage

To update the homepage:

1. Edit `index.html` in this repository
2. Commit and push to the `main` branch
3. The GitHub Actions workflow will automatically deploy the changes
4. Changes should be live within 1-2 minutes

## Customizing the Domain

To change the domain:

1. Update the `CNAME` file with your desired domain
2. Update your DNS records accordingly
3. Wait for DNS propagation
4. Enable HTTPS in repository settings

## Troubleshooting

### DNS Not Propagating
- Use `dig pipestream.ai` or `nslookup pipestream.ai` to check DNS records
- DNS changes can take up to 48 hours to fully propagate
- Use https://dnschecker.org to check propagation globally

### HTTPS Not Available
- Wait for DNS to fully propagate first
- Ensure "Enforce HTTPS" is checked in repository settings
- GitHub may take a few minutes to provision the certificate after DNS is verified

### Page Not Found (404)
- Verify the workflow ran successfully in the Actions tab
- Check that `index.html` exists in the repository root
- Ensure GitHub Pages is set to deploy from "GitHub Actions"

### Custom Domain Not Working
- Verify the CNAME file contains only the domain (no https:// or trailing slash)
- Check that DNS records point to the correct GitHub Pages IPs
- Ensure your domain registrar has propagated the changes

## Architecture

```
┌─────────────────┐
│   Your Domain   │
│  pipestream.ai  │
└────────┬────────┘
         │
         │ DNS Resolution
         ▼
┌─────────────────────────┐
│   GitHub Pages (CDN)    │
│  185.199.108-111.153    │
└────────┬────────────────┘
         │
         │ Serves content from
         ▼
┌─────────────────────────┐
│  .github Repository     │
│  - index.html           │
│  - CNAME                │
│  - Static assets        │
└─────────────────────────┘
         ▲
         │ Deployed by
         │
┌─────────────────────────┐
│  GitHub Actions         │
│  (.github/workflows/)   │
└─────────────────────────┘
```

## Resources

- [GitHub Pages Documentation](https://docs.github.com/en/pages)
- [Configuring a custom domain](https://docs.github.com/en/pages/configuring-a-custom-domain-for-your-github-pages-site)
- [Securing your site with HTTPS](https://docs.github.com/en/pages/getting-started-with-github-pages/securing-your-github-pages-site-with-https)
- [About GitHub Pages and Jekyll](https://docs.github.com/en/pages/setting-up-a-github-pages-site-with-jekyll/about-github-pages-and-jekyll)

## Questions?

If you encounter any issues or have questions about the setup, please open an issue in this repository.
